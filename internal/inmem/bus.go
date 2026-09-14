// Package inmem provides in-memory implementations of port.JobBus and
// port.KV for use in unit tests.
//
// Synchronous semantics: every Publish call delivers to all matching
// subscribers BEFORE returning, so test assertions can run immediately
// after the producer call. This trades production realism for test
// simplicity.
//
// NOT suitable for production: no durability, no clustering, no
// JetStream semantics around retention or dedup beyond a simple
// best-effort msgID set.
//
// Subject matching follows NATS wildcards:
//   - matches a single token
//     > matches one or more tokens (terminal)
package inmem

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/johnnycube/cairn-provider-strava/internal/port"
)

// Bus is the in-memory port.JobBus.
type Bus struct {
	mu sync.Mutex

	// subscribers per subject pattern (literal or with wildcards).
	subs []*subscription

	// request-reply handlers, keyed by literal subject.
	rrHandlers map[string]port.RequestHandler

	// dedup window — simple set keyed by Nats-Msg-Id header value.
	seenMsgIDs map[string]struct{}

	// KV handles, lazy-resolved per bucket.
	kvs map[string]*KV
}

// New constructs an empty in-memory bus.
func New() *Bus {
	return &Bus{
		rrHandlers: map[string]port.RequestHandler{},
		seenMsgIDs: map[string]struct{}{},
		kvs:        map[string]*KV{},
	}
}

type subscription struct {
	subject string // pattern (may contain * or >)
	handler port.MessageHandler
	closed  bool
}

func (b *Bus) Publish(ctx context.Context, subject string, msgID string, body []byte) error {
	b.mu.Lock()
	if msgID != "" {
		if _, dup := b.seenMsgIDs[msgID]; dup {
			b.mu.Unlock()
			return nil // dedup: silently drop
		}
		b.seenMsgIDs[msgID] = struct{}{}
	}

	var matched []*subscription
	for _, s := range b.subs {
		if !s.closed && subjectMatches(s.subject, subject) {
			matched = append(matched, s)
		}
	}
	b.mu.Unlock()

	for _, s := range matched {
		msg := port.Message{
			Subject: subject,
			Body:    body,
			Headers: map[string]string{},
		}
		if msgID != "" {
			msg.Headers["Nats-Msg-Id"] = msgID
		}
		_ = s.handler(ctx, msg) // tests assert on side-effects
	}
	return nil
}

// Subscribe registers a push-style handler for a subject pattern. Test-only
// convenience (not part of port.JobBus): it lets a test observe what the
// worker published without driving a pull loop.
func (b *Bus) Subscribe(
	_ context.Context,
	cfg port.ConsumerConfig,
	handler port.MessageHandler,
) (port.Subscription, error) {
	if cfg.Subject == "" {
		return nil, errors.New("inmem: Subject required")
	}
	s := &subscription{subject: cfg.Subject, handler: handler}
	b.mu.Lock()
	b.subs = append(b.subs, s)
	b.mu.Unlock()
	return &busSubscription{bus: b, sub: s}, nil
}

func (b *Bus) Pull(_ context.Context, _ port.ConsumerConfig) (port.PullSubscription, error) {
	return nil, errors.New("inmem: Pull not supported; use Subscribe")
}

func (b *Bus) Request(
	ctx context.Context,
	subject string,
	body []byte,
	timeout time.Duration,
) ([]byte, error) {
	b.mu.Lock()
	h, ok := b.rrHandlers[subject]
	b.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("inmem: no responder for %s", subject)
	}
	rrCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return h(rrCtx, body)
}

func (b *Bus) RespondTo(
	_ context.Context,
	subject string,
	handler port.RequestHandler,
) (port.Subscription, error) {
	b.mu.Lock()
	b.rrHandlers[subject] = handler
	b.mu.Unlock()
	return &rrSubscription{bus: b, subject: subject}, nil
}

func (b *Bus) KV(bucket string) (port.KV, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if kv, ok := b.kvs[bucket]; ok {
		return kv, nil
	}
	kv := &KV{entries: map[string]*kvEntry{}}
	b.kvs[bucket] = kv
	return kv, nil
}

// ---------------------------------------------------------------------------
// Subscription handles
// ---------------------------------------------------------------------------

type busSubscription struct {
	bus *Bus
	sub *subscription
}

func (s *busSubscription) Close(_ context.Context) error {
	s.bus.mu.Lock()
	s.sub.closed = true
	s.bus.mu.Unlock()
	return nil
}

type rrSubscription struct {
	bus     *Bus
	subject string
}

func (s *rrSubscription) Close(_ context.Context) error {
	s.bus.mu.Lock()
	delete(s.bus.rrHandlers, s.subject)
	s.bus.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------------------
// KV
// ---------------------------------------------------------------------------

type KV struct {
	mu      sync.Mutex
	entries map[string]*kvEntry
	nextRev uint64
}

type kvEntry struct {
	value     []byte
	revision  uint64
	createdAt time.Time
}

func (k *KV) Get(_ context.Context, key string) (port.KVEntry, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	e, ok := k.entries[key]
	if !ok {
		return port.KVEntry{}, port.ErrKVKeyNotFound
	}
	return port.KVEntry{
		Key:       key,
		Value:     append([]byte(nil), e.value...),
		Revision:  e.revision,
		CreatedAt: e.createdAt,
	}, nil
}

func (k *KV) Put(_ context.Context, key string, value []byte) (uint64, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.nextRev++
	k.entries[key] = &kvEntry{
		value:     append([]byte(nil), value...),
		revision:  k.nextRev,
		createdAt: time.Now().UTC(),
	}
	return k.nextRev, nil
}

func (k *KV) CompareAndSet(_ context.Context, key string, value []byte, expectedRev uint64) (uint64, bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	e, ok := k.entries[key]
	if !ok {
		if expectedRev != 0 {
			return 0, false, nil
		}
		k.nextRev++
		k.entries[key] = &kvEntry{
			value:     append([]byte(nil), value...),
			revision:  k.nextRev,
			createdAt: time.Now().UTC(),
		}
		return k.nextRev, true, nil
	}
	if e.revision != expectedRev {
		return 0, false, nil
	}
	k.nextRev++
	e.value = append([]byte(nil), value...)
	e.revision = k.nextRev
	return k.nextRev, true, nil
}

// ---------------------------------------------------------------------------
// Subject matching (NATS-compatible wildcards)
// ---------------------------------------------------------------------------

// subjectMatches returns true if `subject` matches the wildcard pattern
// `pattern`. Tokens are dot-separated; `*` matches one token; `>` matches
// one or more tokens (must be the terminal token of the pattern).
func subjectMatches(pattern, subject string) bool {
	if pattern == subject {
		return true
	}
	pTokens := strings.Split(pattern, ".")
	sTokens := strings.Split(subject, ".")

	for i, pt := range pTokens {
		if pt == ">" {
			return i < len(sTokens)
		}
		if i >= len(sTokens) {
			return false
		}
		if pt == "*" {
			continue
		}
		if pt != sTokens[i] {
			return false
		}
	}
	return len(pTokens) == len(sTokens)
}
