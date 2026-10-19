package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/opencairn-org/cairn-provider-strava/internal/port"
	"github.com/opencairn-org/cairn-provider-strava/internal/workersdk"
)

// tokenServer runs a fake Strava token endpoint and points the handler at it.
func tokenServer(t *testing.T, h http.HandlerFunc) *StravaAuthHandler {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	t.Setenv("CAIRN_STRAVA_TOKEN_URL", srv.URL)
	return NewStravaAuthHandler()
}

var (
	testTokenState = port.TokenState{AccessToken: "old", RefreshToken: "r1", Scope: "read"}
	testCreds      = workersdk.OAuthClientCreds{ClientID: "id", ClientSecret: "secret"}
)

func TestRefresh_Success(t *testing.T) {
	var form url.Values
	h := tokenServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		form, _ = url.ParseQuery(string(body))
		_, _ = io.WriteString(w, `{"token_type":"Bearer","access_token":"new","expires_at":1900000000,"refresh_token":"r2"}`)
	})
	got, err := h.Refresh(context.Background(), testTokenState, testCreds)
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessToken != "new" || got.RefreshToken != "r2" || got.Scope != "read" || got.ExpiresAt.Unix() != 1900000000 {
		t.Errorf("state = %+v", got)
	}
	if form.Get("grant_type") != "refresh_token" || form.Get("refresh_token") != "r1" || form.Get("client_id") != "id" || form.Get("client_secret") != "secret" {
		t.Errorf("form = %v", form)
	}
}

func TestParseStravaRefreshResponse(t *testing.T) {
	got, err := parseStravaRefreshResponse([]byte(`{"access_token":"a","expires_in":60}`), testTokenState)
	if err != nil || got.RefreshToken != "r1" || got.TokenType != "Bearer" || time.Until(got.ExpiresAt) > time.Minute {
		t.Errorf("expires_in: %+v, %v", got, err)
	}
	got, err = parseStravaRefreshResponse([]byte(`{"access_token":"a"}`), testTokenState)
	if err != nil || time.Until(got.ExpiresAt) < 5*time.Hour {
		t.Errorf("default expiry: %+v, %v", got, err)
	}
	if _, err := parseStravaRefreshResponse([]byte(`{}`), testTokenState); err == nil {
		t.Error("missing access_token: want error")
	}
	if _, err := parseStravaRefreshResponse([]byte(`{`), testTokenState); err == nil {
		t.Error("bad json: want error")
	}
}

func TestRefresh_Failures(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		body       string
		wantReason string // terminal reason; "" means a retryable error
	}{
		{"invalid grant", 400, `{"errors":[{"code":"invalid"}]}`, "invalid_grant"},
		{"invalid client", 401, `{"message":"invalid_client"}`, "invalid_client"},
		{"unauthorized client", 400, `unauthorized_client`, "unauthorized_client"},
		{"other rejection", 400, `nope`, "refresh_rejected"},
		{"rate limited", 429, strings.Repeat("x", 300), ""},
		{"server error", 502, "", ""},
		{"unexpected", 418, "teapot", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := tokenServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(c.status)
				_, _ = io.WriteString(w, c.body)
			})
			_, err := h.Refresh(context.Background(), testTokenState, testCreds)
			var term *port.TerminalError
			isTerm := errors.As(err, &term)
			switch {
			case err == nil:
				t.Fatal("want error")
			case c.wantReason == "" && isTerm:
				t.Errorf("err = %v; want retryable", err)
			case c.wantReason != "" && (!isTerm || term.Reason != c.wantReason):
				t.Errorf("err = %v; want terminal %s", err, c.wantReason)
			}
		})
	}
}

func TestRefresh_Preconditions(t *testing.T) {
	h := tokenServer(t, func(http.ResponseWriter, *http.Request) {})
	var term *port.TerminalError
	if _, err := h.Refresh(context.Background(), port.TokenState{}, testCreds); !errors.As(err, &term) || term.Reason != "no_refresh_token" {
		t.Errorf("no refresh token = %v", err)
	}
	if _, err := h.Refresh(context.Background(), testTokenState, workersdk.OAuthClientCreds{}); !errors.As(err, &term) || term.Reason != "missing_client_credentials" {
		t.Errorf("no creds = %v", err)
	}
	h.tokenURL = "http://127.0.0.1:1"
	if _, err := h.Refresh(context.Background(), testTokenState, testCreds); err == nil || errors.As(err, &term) {
		t.Errorf("network error = %v; want retryable", err)
	}
	h.tokenURL = "://bad"
	if _, err := h.Refresh(context.Background(), testTokenState, testCreds); err == nil {
		t.Error("bad url: want error")
	}
}

func TestNewStravaAuthHandler_DefaultURL(t *testing.T) {
	t.Setenv("CAIRN_STRAVA_TOKEN_URL", "")
	if h := NewStravaAuthHandler(); h.tokenURL != "https://www.strava.com/oauth/token" {
		t.Errorf("tokenURL = %q", h.tokenURL)
	}
	if truncate([]byte("abc"), 2) != "ab…" || truncate([]byte("abc"), 5) != "abc" {
		t.Error("truncate wrong")
	}
}
