package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/johnnycube/cairn-provider-strava/internal/port"
	"github.com/johnnycube/cairn-provider-strava/internal/workersdk"
)

// StravaAuthHandler implements workersdk.AuthHandler for Strava's OAuth2
// refresh flow. See https://developers.strava.com/docs/authentication/
// for the canonical reference.
//
// The client_id/client_secret are NOT held by the worker: Cairn's provider
// credentials are per-user, so the server passes the owning user's OAuth app
// creds into Refresh (workersdk.OAuthClientCreds) alongside the token. The
// worker only knows the token endpoint (static, overridable for tests via
// CAIRN_STRAVA_TOKEN_URL).
//
// Strava's refresh response:
//
//	POST https://www.strava.com/oauth/token
//	Content-Type: application/x-www-form-urlencoded
//	Body: client_id=X&client_secret=Y&grant_type=refresh_token&refresh_token=Z
//
//	200 OK
//	{
//	  "token_type":     "Bearer",
//	  "access_token":   "...",
//	  "expires_at":     1715912345,   // absolute, unix seconds
//	  "expires_in":     21600,        // relative, seconds
//	  "refresh_token": "..."          // sometimes rotated, sometimes same
//	}
//
// Error cases:
//
//	400 invalid_grant — refresh token revoked/expired → TerminalError("invalid_grant")
//	400 invalid_request / invalid_client → TerminalError (config bug, won't fix on retry)
//	401 — same as invalid_grant in practice
//	429 / 5xx / network — transient (plain error → SDK NAKs the in-flight job)
type StravaAuthHandler struct {
	tokenURL string
	http     *http.Client
}

// NewStravaAuthHandler constructs the handler. No credentials are read from
// env — they arrive per-refresh from the server (per-user model). Only the
// token endpoint is configurable, for tests.
func NewStravaAuthHandler() (*StravaAuthHandler, error) {
	tokenURL := os.Getenv("CAIRN_STRAVA_TOKEN_URL")
	if tokenURL == "" {
		tokenURL = "https://www.strava.com/oauth/token"
	}
	return &StravaAuthHandler{
		tokenURL: tokenURL,
		http: &http.Client{
			Timeout: 15 * time.Second,
		},
	}, nil
}

// Refresh implements workersdk.AuthHandler.
func (h *StravaAuthHandler) Refresh(
	ctx context.Context,
	current port.TokenState,
	creds workersdk.OAuthClientCreds,
) (port.TokenState, error) {
	if current.RefreshToken == "" {
		return port.TokenState{}, &port.TerminalError{
			Reason: "no_refresh_token",
			Cause:  errors.New("token state missing refresh_token"),
		}
	}
	if creds.ClientID == "" || creds.ClientSecret == "" {
		return port.TokenState{}, &port.TerminalError{
			Reason: "missing_client_credentials",
			Cause:  errors.New("server returned no per-user OAuth app credentials for account"),
		}
	}

	form := url.Values{}
	form.Set("client_id", creds.ClientID)
	form.Set("client_secret", creds.ClientSecret)
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", current.RefreshToken)

	req, err := http.NewRequestWithContext(ctx, "POST", h.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return port.TokenState{}, fmt.Errorf("build refresh request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := h.http.Do(req)
	if err != nil {
		return port.TokenState{}, fmt.Errorf("strava refresh HTTP: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))

	switch {
	case resp.StatusCode == 200:
		return parseStravaRefreshResponse(body, current)

	case resp.StatusCode == 400 || resp.StatusCode == 401:
		// Strava's 400 body contains:
		//   {"message":"Bad Request","errors":[{"resource":"RefreshToken","field":"refresh_token","code":"invalid"}]}
		reason := classifyStravaError(body)
		return port.TokenState{}, &port.TerminalError{
			Reason: reason,
			Cause:  fmt.Errorf("strava refresh %d: %s", resp.StatusCode, truncate(body, 200)),
		}

	case resp.StatusCode == 429:
		return port.TokenState{}, fmt.Errorf("strava refresh rate-limited (429): %s", truncate(body, 100))

	case resp.StatusCode >= 500:
		return port.TokenState{}, fmt.Errorf("strava refresh server error %d: %s", resp.StatusCode, truncate(body, 100))

	default:
		return port.TokenState{}, fmt.Errorf("strava refresh unexpected %d: %s", resp.StatusCode, truncate(body, 200))
	}
}

// stravaRefreshResponse is the JSON shape Strava returns on a successful
// refresh.
type stravaRefreshResponse struct {
	TokenType    string `json:"token_type"`
	AccessToken  string `json:"access_token"`
	ExpiresAt    int64  `json:"expires_at"`    // unix seconds, absolute
	ExpiresIn    int64  `json:"expires_in"`    // seconds, relative
	RefreshToken string `json:"refresh_token"` // sometimes rotated
}

func parseStravaRefreshResponse(body []byte, current port.TokenState) (port.TokenState, error) {
	var r stravaRefreshResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return port.TokenState{}, fmt.Errorf("decode strava refresh response: %w", err)
	}
	if r.AccessToken == "" {
		return port.TokenState{}, errors.New("strava refresh response missing access_token")
	}

	var expiresAt time.Time
	switch {
	case r.ExpiresAt > 0:
		expiresAt = time.Unix(r.ExpiresAt, 0).UTC()
	case r.ExpiresIn > 0:
		expiresAt = time.Now().UTC().Add(time.Duration(r.ExpiresIn) * time.Second)
	default:
		// Strava normally always sends both; defensive fallback.
		expiresAt = time.Now().UTC().Add(6 * time.Hour)
	}

	refresh := r.RefreshToken
	if refresh == "" {
		// Strava sometimes returns no refresh_token when it's unchanged.
		refresh = current.RefreshToken
	}
	tokenType := r.TokenType
	if tokenType == "" {
		tokenType = "Bearer"
	}

	return port.TokenState{
		AccessToken:  r.AccessToken,
		RefreshToken: refresh,
		ExpiresAt:    expiresAt,
		Scope:        current.Scope, // Strava doesn't return scope on refresh
		TokenType:    tokenType,
	}, nil
}

// classifyStravaError maps a 400/401 body to a stable error reason
// string for the TerminalError. Strava error bodies vary; we look for
// the most common discriminators.
func classifyStravaError(body []byte) string {
	s := strings.ToLower(string(body))
	switch {
	case strings.Contains(s, "invalid_grant"), strings.Contains(s, "invalid"):
		// "invalid" appears in their per-field error code for revoked
		// refresh tokens. Both flavours mean "user must reauth".
		return "invalid_grant"
	case strings.Contains(s, "invalid_client"):
		return "invalid_client" // config bug
	case strings.Contains(s, "unauthorized_client"):
		return "unauthorized_client"
	default:
		return "refresh_rejected"
	}
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}

// compile-time assertion that StravaAuthHandler satisfies AuthHandler.
var _ workersdk.AuthHandler = (*StravaAuthHandler)(nil)
