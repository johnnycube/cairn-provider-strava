package workersdk

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/opencairn-org/cairn-provider-strava/internal/inmem"
	"github.com/opencairn-org/cairn-provider-strava/internal/port"
)

// expiredGet answers token gets with an expired state.
func expiredGet(bus *inmem.Bus) {
	_, _ = bus.RespondTo(context.Background(), "cairn.tokens.p.get", func(context.Context, []byte) ([]byte, error) {
		return json.Marshal(tokenGetReply{AccessToken: "old", RefreshToken: "r", ExpiresAt: time.Now().Add(-time.Minute)})
	})
}

func TestTokenCache_CachesFreshToken(t *testing.T) {
	bus := inmem.New()
	calls := 0
	_, _ = bus.RespondTo(context.Background(), "cairn.tokens.p.get", func(context.Context, []byte) ([]byte, error) {
		calls++
		return json.Marshal(tokenGetReply{AccessToken: "a", ExpiresAt: time.Now().Add(time.Hour)})
	})
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus})
	for i := 0; i < 3; i++ {
		if tok, err := w.FetchToken(context.Background(), "acc"); err != nil || tok.AccessToken != "a" {
			t.Fatalf("FetchToken = %+v, %v", tok, err)
		}
	}
	if calls != 1 {
		t.Errorf("server gets = %d; want 1 (cached)", calls)
	}
}

func TestTokenCache_GetErrors(t *testing.T) {
	ctx := context.Background()
	bus := inmem.New()
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus})
	if _, err := w.FetchToken(ctx, "acc"); err == nil || !strings.Contains(err.Error(), "token get RPC") {
		t.Errorf("no responder = %v", err)
	}
	_, _ = bus.RespondTo(ctx, "cairn.tokens.p.get", func(context.Context, []byte) ([]byte, error) { return []byte("{"), nil })
	if _, err := w.FetchToken(ctx, "acc"); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Errorf("bad reply = %v", err)
	}
	expiredGet(bus)
	if _, err := w.FetchToken(ctx, "acc"); err == nil || !strings.Contains(err.Error(), "no AuthHandler") {
		t.Errorf("no auth = %v", err)
	}
}

func TestTokenCache_RefreshTransientError(t *testing.T) {
	bus := inmem.New()
	expiredGet(bus)
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus, Auth: &stubAuth{NextErr: errors.New("503")}})
	_, err := w.FetchToken(context.Background(), "acc")
	var term *port.TerminalError
	if err == nil || errors.As(err, &term) || !strings.Contains(err.Error(), "refresh failed") {
		t.Fatalf("err = %v; want a plain refresh failure", err)
	}
}

func TestTokenCache_StoreOutcomes(t *testing.T) {
	cases := []struct {
		name  string
		reply func() ([]byte, error)
		check func(t *testing.T, err error)
	}{
		{"rpc error", func() ([]byte, error) { return nil, errors.New("down") }, func(t *testing.T, err error) {
			if err == nil || !strings.Contains(err.Error(), "token store RPC") {
				t.Errorf("err = %v", err)
			}
		}},
		{"bad reply", func() ([]byte, error) { return []byte("{"), nil }, func(t *testing.T, err error) {
			if err == nil || !strings.Contains(err.Error(), "token store decode") {
				t.Errorf("err = %v", err)
			}
		}},
		{"account gone", func() ([]byte, error) { return json.Marshal(tokenStoreReply{Error: "account_gone"}) }, func(t *testing.T, err error) {
			var term *port.TerminalError
			if !errors.As(err, &term) || term.Reason != "account_gone" {
				t.Errorf("err = %v", err)
			}
		}},
		{"other", func() ([]byte, error) { return json.Marshal(tokenStoreReply{Error: "weird"}) }, func(t *testing.T, err error) {
			if err == nil || !strings.Contains(err.Error(), "weird") {
				t.Errorf("err = %v", err)
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bus := inmem.New()
			expiredGet(bus)
			_, _ = bus.RespondTo(context.Background(), "cairn.tokens.p.store", func(context.Context, []byte) ([]byte, error) { return c.reply() })
			auth := &stubAuth{NextState: port.TokenState{AccessToken: "new", ExpiresAt: time.Now().Add(time.Hour)}}
			w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus, Auth: auth})
			_, err := w.FetchToken(context.Background(), "acc")
			c.check(t, err)
		})
	}
}

func TestTokenCache_StaleStoreRefetchFails(t *testing.T) {
	bus := inmem.New()
	gets := 0
	_, _ = bus.RespondTo(context.Background(), "cairn.tokens.p.get", func(context.Context, []byte) ([]byte, error) {
		gets++
		if gets > 1 {
			return json.Marshal(tokenGetReply{Error: "needs_reauth"})
		}
		return json.Marshal(tokenGetReply{AccessToken: "old", ExpiresAt: time.Now().Add(-time.Minute)})
	})
	_, _ = bus.RespondTo(context.Background(), "cairn.tokens.p.store", func(context.Context, []byte) ([]byte, error) {
		return json.Marshal(tokenStoreReply{Error: "stale_store"})
	})
	auth := &stubAuth{NextState: port.TokenState{AccessToken: "new", ExpiresAt: time.Now().Add(time.Hour)}}
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus, Auth: auth})
	_, err := w.FetchToken(context.Background(), "acc")
	var term *port.TerminalError
	if !errors.As(err, &term) || term.Reason != "needs_reauth" {
		t.Fatalf("err = %v", err)
	}
}

func TestMapReplyError(t *testing.T) {
	var nak *port.NakWithDelayError
	if err := mapReplyError("transient", 0); !errors.As(err, &nak) || nak.Delay != 30*time.Second {
		t.Errorf("transient default delay: %v", err)
	}
	if err := mapReplyError("needs_reauth", 0); err == nil || !strings.Contains(err.Error(), "needs_reauth") {
		t.Errorf("needs_reauth: %v", err)
	}
	if err := mapReplyError("odd", 0); err == nil || !strings.Contains(err.Error(), "odd") {
		t.Errorf("unknown: %v", err)
	}
}
