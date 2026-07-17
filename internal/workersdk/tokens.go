package workersdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/johnnycube/cairn-provider-strava/internal/port"
)

// ---------------------------------------------------------------------------
// AuthHandler — provider-specific OAuth refresh logic
//
// The SDK calls Refresh whenever the cached AccessToken is expired (or
// expiring within refreshSkew). The worker's concrete implementation
// (e.g. cmd/worker-strava/auth.go's StravaAuthHandler) knows the
// provider's refresh endpoint, body shape, and error semantics.
//
// This is THE only worker-side abstraction the core defines for auth.
// Everything else — endpoints, scopes, client_id/secret, error strings
// — lives in the worker's own package.
//
// Threat-model note: the SDK passes the FULL TokenState (incl. refresh
// token) to Refresh. The worker is trusted to handle it correctly; if
// the worker is compromised, refresh tokens leak. NATS Account isolation
// (architecture.md §4) is what prevents cross-provider blast radius.
// ---------------------------------------------------------------------------

// AuthHandler is implemented by each provider-specific worker.
//
// Refresh trades the current token state for a fresh one by calling
// the provider's OAuth refresh endpoint. Returns a wrapped
// *port.TerminalError when the provider permanently rejects
// (invalid_grant, expired refresh token, revoked grant); the SDK then
// flags the account needs_reauth on the server and Term()s the
// in-flight job.
//
// Implementations should:
//   - Use the AccessToken's provider for the HTTP call (no provider
//     argument needed; the worker is provider-specific by definition).
//   - Set TokenType="Bearer" on the returned state if the provider
//     doesn't specify.
//   - Preserve Scope unless the provider returned a new one.
//   - Compute ExpiresAt as now + provider's expires_in.
type AuthHandler interface {
	Refresh(ctx context.Context, current port.TokenState, creds OAuthClientCreds) (port.TokenState, error)
}

// OAuthClientCreds is the per-account OAuth app identity (the user's own
// client_id/client_secret) the server returns alongside the token. Provider
// credentials are per-user in Cairn — there is no instance-global app — so the
// worker cannot hold them in env; it receives them per refresh from the server,
// which stores them encrypted (user_provider_configs).
type OAuthClientCreds struct {
	ClientID     string
	ClientSecret string
}

// ---------------------------------------------------------------------------
// Token cache — worker-driven refresh
// ---------------------------------------------------------------------------

const refreshSkew = 60 * time.Second

// Token is the value handlers receive. Only AccessToken is exposed —
// they shouldn't need the refresh token for normal API calls.
type Token struct {
	AccessToken string
	ExpiresAt   time.Time
	Scope       string
}

// tokenCache caches the FULL TokenState per account in-process and
// proactively refreshes via the AuthHandler when expiry is near.
//
// Server flow per Get call:
//  1. Cache hit + not expiring? Return.
//  2. Cache miss or expiring? RPC cairn.tokens.<provider>.get to get
//     the latest state from the server.
//  3. Still expiring (or fresh from server but already expired)?
//     Call auth.Refresh(state).
//  4. Refresh OK? RPC cairn.tokens.<provider>.store to persist.
//     On stale_store, re-fetch and use whatever the other worker wrote.
//  5. Refresh permanently failed? RPC cairn.tokens.<provider>.needs_reauth,
//     return *port.TerminalError so the in-flight handler Term()s.
type tokenCache struct {
	bus      port.JobBus
	provider string
	auth     AuthHandler // nil → no refresh ability; pure read-through cache
	timeout  time.Duration
	logger   *slog.Logger

	mu    sync.Mutex
	cache map[string]*cacheEntry
}

type cacheEntry struct {
	state     port.TokenState
	creds     OAuthClientCreds // per-account OAuth app creds from the last server get
	refreshMu sync.Mutex       // serialises refresh attempts for one account
}

func newTokenCache(
	bus port.JobBus,
	provider string,
	auth AuthHandler,
	timeout time.Duration,
	logger *slog.Logger,
) *tokenCache {
	return &tokenCache{
		bus:      bus,
		provider: provider,
		auth:     auth,
		timeout:  timeout,
		logger:   logger.With("component", "token_cache"),
		cache:    map[string]*cacheEntry{},
	}
}

// Get returns a valid access token for the account. Refreshes via the
// AuthHandler if needed.
func (c *tokenCache) Get(ctx context.Context, accountID string) (Token, error) {
	entry := c.entryFor(accountID)

	entry.refreshMu.Lock()
	defer entry.refreshMu.Unlock()

	// Fast-path: cached + fresh enough? Return without touching server.
	if hasFresh(entry.state) {
		return tokenFromState(entry.state), nil
	}

	// Pull latest from server (might have been refreshed by another instance).
	// The reply also carries the account's per-user OAuth app creds, which the
	// AuthHandler needs to talk to the provider's token endpoint.
	state, creds, err := c.serverGet(ctx, accountID)
	if err != nil {
		return Token{}, err
	}
	entry.state = state
	entry.creds = creds

	if hasFresh(state) {
		return tokenFromState(state), nil
	}

	// Need to actually refresh.
	if c.auth == nil {
		return Token{}, fmt.Errorf("token cache: no AuthHandler configured for provider %q; cannot refresh", c.provider)
	}

	c.logger.Info("refreshing OAuth token",
		"account_id", accountID,
		"current_expires_at", state.ExpiresAt,
	)
	fresh, err := c.auth.Refresh(ctx, state, entry.creds)
	if err != nil {
		var term *port.TerminalError
		if errors.As(err, &term) {
			_ = c.serverMarkNeedsReauth(ctx, accountID, term.Reason)
			return Token{}, err
		}
		return Token{}, fmt.Errorf("refresh failed: %w", err)
	}

	// Store the new state. On stale_store (another instance won), refetch.
	stored, err := c.serverStore(ctx, accountID, fresh, state.ExpiresAt)
	if err != nil {
		return Token{}, err
	}
	if stored != nil {
		entry.state = *stored
	} else {
		entry.state = fresh
	}
	return tokenFromState(entry.state), nil
}

// Invalidate drops cached state. Called when an API call surfaces a 401
// despite having a fresh-looking token — forces re-fetch on next Get.
func (c *tokenCache) Invalidate(accountID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, accountID)
}

func (c *tokenCache) entryFor(accountID string) *cacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.cache[accountID]
	if !ok {
		e = &cacheEntry{}
		c.cache[accountID] = e
	}
	return e
}

func hasFresh(s port.TokenState) bool {
	return s.AccessToken != "" && time.Until(s.ExpiresAt) > refreshSkew
}

func tokenFromState(s port.TokenState) Token {
	return Token{
		AccessToken: s.AccessToken,
		ExpiresAt:   s.ExpiresAt,
		Scope:       s.Scope,
	}
}

// ---------------------------------------------------------------------------
// Server RPCs
// ---------------------------------------------------------------------------

func (c *tokenCache) serverGet(ctx context.Context, accountID string) (port.TokenState, OAuthClientCreds, error) {
	body, _ := json.Marshal(map[string]string{"account_id": accountID})
	subj := "cairn.tokens." + c.provider + ".get"
	resp, err := c.bus.Request(ctx, subj, body, c.timeout)
	if err != nil {
		return port.TokenState{}, OAuthClientCreds{}, fmt.Errorf("token get RPC: %w", err)
	}
	var reply tokenGetReply
	if err := json.Unmarshal(resp, &reply); err != nil {
		return port.TokenState{}, OAuthClientCreds{}, fmt.Errorf("token get decode: %w", err)
	}
	if reply.Error != "" {
		return port.TokenState{}, OAuthClientCreds{}, mapReplyError(reply.Error, reply.RetryAfter)
	}
	return port.TokenState{
			AccessToken:  reply.AccessToken,
			RefreshToken: reply.RefreshToken,
			ExpiresAt:    reply.ExpiresAt,
			Scope:        reply.Scope,
			TokenType:    reply.TokenType,
		}, OAuthClientCreds{
			ClientID:     reply.ClientID,
			ClientSecret: reply.ClientSecret,
		}, nil
}

// serverStore returns:
//   - (nil, nil) on successful write — caller uses the state it sent.
//   - (&state, nil) when the server reported stale_store — caller uses the
//     returned (winning) state.
//   - (nil, err) on transient/error.
func (c *tokenCache) serverStore(
	ctx context.Context,
	accountID string,
	state port.TokenState,
	previousExpiresAt time.Time,
) (*port.TokenState, error) {
	body, _ := json.Marshal(tokenStoreRequest{
		AccountID:         accountID,
		AccessToken:       state.AccessToken,
		RefreshToken:      state.RefreshToken,
		ExpiresAt:         state.ExpiresAt,
		Scope:             state.Scope,
		TokenType:         state.TokenType,
		PreviousExpiresAt: previousExpiresAt,
	})
	subj := "cairn.tokens." + c.provider + ".store"
	resp, err := c.bus.Request(ctx, subj, body, c.timeout)
	if err != nil {
		return nil, fmt.Errorf("token store RPC: %w", err)
	}
	var reply tokenStoreReply
	if err := json.Unmarshal(resp, &reply); err != nil {
		return nil, fmt.Errorf("token store decode: %w", err)
	}
	if reply.OK {
		return nil, nil
	}
	switch reply.Error {
	case "stale_store":
		c.logger.Debug("token store stale; refetching", "account_id", accountID)
		st, _, err := c.serverGet(ctx, accountID)
		if err != nil {
			return nil, err
		}
		return &st, nil
	case "account_gone":
		return nil, &port.TerminalError{Reason: "account_gone", Cause: errors.New("account no longer exists")}
	default:
		return nil, fmt.Errorf("token store error: %s", reply.Error)
	}
}

func (c *tokenCache) serverMarkNeedsReauth(ctx context.Context, accountID, reason string) error {
	body, _ := json.Marshal(map[string]string{
		"account_id": accountID,
		"reason":     reason,
	})
	subj := "cairn.tokens." + c.provider + ".needs_reauth"
	_, err := c.bus.Request(ctx, subj, body, c.timeout)
	return err
}

func mapReplyError(reason string, retryAfterSeconds int) error {
	switch reason {
	case "account_gone":
		return &port.TerminalError{Reason: "account_gone", Cause: errors.New("account not found")}
	case "needs_reauth":
		return &port.TerminalError{Reason: "needs_reauth", Cause: errors.New("account needs re-authorization")}
	case "transient":
		delay := time.Duration(retryAfterSeconds) * time.Second
		if delay <= 0 {
			delay = 30 * time.Second
		}
		return &port.NakWithDelayError{Reason: "token_fetch_transient", Delay: delay}
	default:
		return fmt.Errorf("token RPC error: %s", reason)
	}
}

// ---------------------------------------------------------------------------
// Wire types — must match cmd/server/oauth_token_handler.go
// ---------------------------------------------------------------------------

type tokenGetReply struct {
	AccessToken  string    `json:"access_token,omitempty"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	ExpiresAt    time.Time `json:"expires_at,omitempty"`
	Scope        string    `json:"scope,omitempty"`
	TokenType    string    `json:"token_type,omitempty"`
	ClientID     string    `json:"client_id,omitempty"`
	ClientSecret string    `json:"client_secret,omitempty"`
	Error        string    `json:"error,omitempty"`
	RetryAfter   int       `json:"retry_after,omitempty"`
}

type tokenStoreRequest struct {
	AccountID         string    `json:"account_id"`
	AccessToken       string    `json:"access_token"`
	RefreshToken      string    `json:"refresh_token,omitempty"`
	ExpiresAt         time.Time `json:"expires_at"`
	Scope             string    `json:"scope,omitempty"`
	TokenType         string    `json:"token_type,omitempty"`
	PreviousExpiresAt time.Time `json:"previous_expires_at"`
}

type tokenStoreReply struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}
