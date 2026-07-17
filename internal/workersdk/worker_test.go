package workersdk

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johnnycube/cairn-provider-strava/internal/inmem"
	"github.com/johnnycube/cairn-provider-strava/internal/port"
)

// stubAuth is a controllable AuthHandler for tests.
type stubAuth struct {
	mu        sync.Mutex
	Calls     int
	LastCreds OAuthClientCreds
	NextState port.TokenState
	NextErr   error
}

func (a *stubAuth) Refresh(_ context.Context, _ port.TokenState, creds OAuthClientCreds) (port.TokenState, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Calls++
	a.LastCreds = creds
	if a.NextErr != nil {
		return port.TokenState{}, a.NextErr
	}
	return a.NextState, nil
}

// ---------------------------------------------------------------------------
// Token cache tests — worker-driven flow
// ---------------------------------------------------------------------------

func TestTokenCache_ServerHasFreshState_NoRefreshNeeded(t *testing.T) {
	bus := inmem.New()
	ctx := context.Background()

	_, err := bus.RespondTo(ctx, "cairn.tokens.strava.get", func(_ context.Context, _ []byte) ([]byte, error) {
		return json.Marshal(tokenGetReply{
			AccessToken: "long-lived",
			ExpiresAt:   time.Now().Add(2 * time.Hour),
			Scope:       "read,activity:read_all",
		})
	})
	if err != nil {
		t.Fatalf("RespondTo: %v", err)
	}

	auth := &stubAuth{}
	w, _ := New(Config{Name: "strava-fetcher", Provider: "strava", Bus: bus, Auth: auth})

	tok, err := w.FetchToken(ctx, "acc-1")
	if err != nil {
		t.Fatalf("FetchToken: %v", err)
	}
	if tok.AccessToken != "long-lived" {
		t.Errorf("AccessToken = %q", tok.AccessToken)
	}
	if auth.Calls != 0 {
		t.Errorf("AuthHandler.Refresh should not have been called; got %d calls", auth.Calls)
	}
}

func TestTokenCache_ServerHasExpiredState_TriggersRefresh(t *testing.T) {
	bus := inmem.New()
	ctx := context.Background()
	var storeCalls int32

	var current port.TokenState
	current.AccessToken = "expired"
	current.ExpiresAt = time.Now().Add(-1 * time.Minute)
	current.RefreshToken = "refresh-1"

	_, _ = bus.RespondTo(ctx, "cairn.tokens.strava.get", func(_ context.Context, _ []byte) ([]byte, error) {
		return json.Marshal(tokenGetReply{
			AccessToken:  current.AccessToken,
			RefreshToken: current.RefreshToken,
			ExpiresAt:    current.ExpiresAt,
			ClientID:     "49963",
			ClientSecret: "user-secret",
		})
	})
	_, _ = bus.RespondTo(ctx, "cairn.tokens.strava.store", func(_ context.Context, body []byte) ([]byte, error) {
		atomic.AddInt32(&storeCalls, 1)
		var req tokenStoreRequest
		_ = json.Unmarshal(body, &req)
		current.AccessToken = req.AccessToken
		current.ExpiresAt = req.ExpiresAt
		current.RefreshToken = req.RefreshToken
		return json.Marshal(tokenStoreReply{OK: true})
	})

	auth := &stubAuth{NextState: port.TokenState{
		AccessToken:  "freshly-refreshed",
		RefreshToken: "refresh-2",
		ExpiresAt:    time.Now().Add(2 * time.Hour),
	}}
	w, _ := New(Config{Name: "strava-fetcher", Provider: "strava", Bus: bus, Auth: auth})

	tok, err := w.FetchToken(ctx, "acc-1")
	if err != nil {
		t.Fatalf("FetchToken: %v", err)
	}
	if tok.AccessToken != "freshly-refreshed" {
		t.Errorf("AccessToken = %q, want freshly-refreshed", tok.AccessToken)
	}
	if auth.Calls != 1 {
		t.Errorf("Refresh calls = %d, want 1", auth.Calls)
	}
	if auth.LastCreds.ClientID != "49963" || auth.LastCreds.ClientSecret != "user-secret" {
		t.Errorf("Refresh got creds %+v, want per-user client_id/secret from server reply", auth.LastCreds)
	}
	if got := atomic.LoadInt32(&storeCalls); got != 1 {
		t.Errorf("store calls = %d, want 1", got)
	}
}

func TestTokenCache_RefreshReturnsTerminalError_MarksNeedsReauth(t *testing.T) {
	bus := inmem.New()
	ctx := context.Background()

	_, _ = bus.RespondTo(ctx, "cairn.tokens.strava.get", func(_ context.Context, _ []byte) ([]byte, error) {
		return json.Marshal(tokenGetReply{
			AccessToken:  "expired",
			ExpiresAt:    time.Now().Add(-1 * time.Minute),
			RefreshToken: "x",
		})
	})
	var reauthCalls int32
	_, _ = bus.RespondTo(ctx, "cairn.tokens.strava.needs_reauth", func(_ context.Context, _ []byte) ([]byte, error) {
		atomic.AddInt32(&reauthCalls, 1)
		return json.Marshal(map[string]bool{"ok": true})
	})

	auth := &stubAuth{NextErr: &port.TerminalError{Reason: "invalid_grant"}}
	w, _ := New(Config{Name: "strava-fetcher", Provider: "strava", Bus: bus, Auth: auth})

	_, err := w.FetchToken(ctx, "acc-1")
	var term *port.TerminalError
	if !errors.As(err, &term) {
		t.Fatalf("expected *port.TerminalError, got %T: %v", err, err)
	}
	if term.Reason != "invalid_grant" {
		t.Errorf("reason = %q", term.Reason)
	}
	if got := atomic.LoadInt32(&reauthCalls); got != 1 {
		t.Errorf("needs_reauth RPC calls = %d, want 1", got)
	}
}

func TestTokenCache_ServerReturnsAccountGone(t *testing.T) {
	bus := inmem.New()
	ctx := context.Background()

	_, _ = bus.RespondTo(ctx, "cairn.tokens.strava.get", func(_ context.Context, _ []byte) ([]byte, error) {
		return json.Marshal(tokenGetReply{Error: "account_gone"})
	})

	w, _ := New(Config{Name: "strava-fetcher", Provider: "strava", Bus: bus, Auth: &stubAuth{}})

	_, err := w.FetchToken(ctx, "acc-1")
	var term *port.TerminalError
	if !errors.As(err, &term) {
		t.Fatalf("expected *port.TerminalError, got %T: %v", err, err)
	}
	if term.Reason != "account_gone" {
		t.Errorf("reason = %q", term.Reason)
	}
}

func TestTokenCache_ServerReturnsTransient_NakWithDelay(t *testing.T) {
	bus := inmem.New()
	ctx := context.Background()

	_, _ = bus.RespondTo(ctx, "cairn.tokens.strava.get", func(_ context.Context, _ []byte) ([]byte, error) {
		return json.Marshal(tokenGetReply{Error: "transient", RetryAfter: 45})
	})

	w, _ := New(Config{Name: "strava-fetcher", Provider: "strava", Bus: bus, Auth: &stubAuth{}})

	_, err := w.FetchToken(ctx, "acc-1")
	var nak *port.NakWithDelayError
	if !errors.As(err, &nak) {
		t.Fatalf("expected *port.NakWithDelayError, got %T: %v", err, err)
	}
	if nak.Delay != 45*time.Second {
		t.Errorf("delay = %v", nak.Delay)
	}
}

func TestTokenCache_StaleStore_RefetchesAndUsesWinningState(t *testing.T) {
	bus := inmem.New()
	ctx := context.Background()
	var getCalls int32

	_, _ = bus.RespondTo(ctx, "cairn.tokens.strava.get", func(_ context.Context, _ []byte) ([]byte, error) {
		n := atomic.AddInt32(&getCalls, 1)
		if n == 1 {
			return json.Marshal(tokenGetReply{
				AccessToken:  "expired",
				ExpiresAt:    time.Now().Add(-1 * time.Minute),
				RefreshToken: "x",
			})
		}
		return json.Marshal(tokenGetReply{
			AccessToken: "winning-from-other-worker",
			ExpiresAt:   time.Now().Add(2 * time.Hour),
		})
	})
	_, _ = bus.RespondTo(ctx, "cairn.tokens.strava.store", func(_ context.Context, _ []byte) ([]byte, error) {
		return json.Marshal(tokenStoreReply{Error: "stale_store"})
	})

	auth := &stubAuth{NextState: port.TokenState{
		AccessToken:  "our-refreshed",
		ExpiresAt:    time.Now().Add(2 * time.Hour),
		RefreshToken: "y",
	}}
	w, _ := New(Config{Name: "strava-fetcher", Provider: "strava", Bus: bus, Auth: auth})

	tok, err := w.FetchToken(ctx, "acc-1")
	if err != nil {
		t.Fatalf("FetchToken: %v", err)
	}
	if tok.AccessToken != "winning-from-other-worker" {
		t.Errorf("AccessToken = %q, want winning-from-other-worker", tok.AccessToken)
	}
}

// ---------------------------------------------------------------------------
// Rate-limit reserve tests
// ---------------------------------------------------------------------------

type stubLimiter struct {
	mu         sync.Mutex
	Allow      bool
	RetryAfter time.Duration
	Refilled   bool
}

func (s *stubLimiter) Reserve(_ context.Context, _ string, _ int) (bool, time.Duration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Allow, s.RetryAfter, nil
}

func (s *stubLimiter) ForceRefill(_ context.Context, _ string, _ int, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Refilled = true
	return nil
}

func (s *stubLimiter) SyncUsage(_ context.Context, _ string, _, _ int, _ time.Time) error {
	return nil
}

func (s *stubLimiter) Snapshot(_ context.Context, bucket string) (port.BucketSnapshot, error) {
	return port.BucketSnapshot{Bucket: bucket}, nil
}

func TestReserveAPI_Allow(t *testing.T) {
	bus := inmem.New()
	limiter := &stubLimiter{Allow: true}
	w, _ := New(Config{Name: "strava-fetcher", Provider: "strava", Bus: bus, Limiter: limiter})

	if err := w.ReserveAPI(context.Background(), "strava:short", 1); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestReserveAPI_Exhausted_ReturnsNakWithDelay(t *testing.T) {
	bus := inmem.New()
	limiter := &stubLimiter{Allow: false, RetryAfter: 3 * time.Minute}
	w, _ := New(Config{Name: "strava-fetcher", Provider: "strava", Bus: bus, Limiter: limiter})

	err := w.ReserveAPI(context.Background(), "strava:short", 1)
	var nak *port.NakWithDelayError
	if !errors.As(err, &nak) {
		t.Fatalf("expected *port.NakWithDelayError, got %T: %v", err, err)
	}
	if nak.Delay != 3*time.Minute {
		t.Errorf("delay = %v", nak.Delay)
	}
	if nak.Reason != "rate_limited" {
		t.Errorf("reason = %q", nak.Reason)
	}
}

// ---------------------------------------------------------------------------
// Heartbeat + helpers
// ---------------------------------------------------------------------------

func TestHeartbeat_PublishesToKV(t *testing.T) {
	bus := inmem.New()
	ctx := context.Background()

	w, _ := New(Config{
		Name:       "strava-fetcher",
		InstanceID: "test-instance",
		Version:    "v0.4.2",
		Package:    "cairns.internal.strava-importer",
		Provider:   "strava",
		Bus:        bus,
	})

	if err := w.publishHeartbeat(ctx); err != nil {
		t.Fatalf("publishHeartbeat: %v", err)
	}

	kv, _ := bus.KV(kvWorkerPresence)
	entry, err := kv.Get(ctx, "strava-fetcher.test-instance")
	if err != nil {
		t.Fatalf("kv get: %v", err)
	}
	var hb heartbeatPayload
	if err := json.Unmarshal(entry.Value, &hb); err != nil {
		t.Fatalf("decode heartbeat: %v", err)
	}
	if hb.WorkerName != "strava-fetcher" || hb.InstanceID != "test-instance" {
		t.Errorf("heartbeat fields wrong: %+v", hb)
	}
}

func TestDeriveResultSubject(t *testing.T) {
	if got := deriveResultSubject("cairn.jobs.fetch_source.strava"); got != "cairn.results.fetch_source.strava" {
		t.Errorf("got %q", got)
	}
	if got := deriveResultSubject("cairn.cmd.foo"); got != "cairn.cmd.foo.result" {
		t.Errorf("got %q", got)
	}
}
