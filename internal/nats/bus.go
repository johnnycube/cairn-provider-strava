// Package nats provides the NATS+JetStream implementations of port.JobBus
// (pull consumers, publish, request/reply, KV handles) and port.RateLimiter
// that the worker runs on.
//
// Imports of nats.go are confined to this package — port stays free of
// nats.* types. The Bus is the only object the rest of the binary sees;
// everything else is reached through port interfaces.
//
// See cairn-core docs/architecture.md §1–§5 for the design behind subject
// layout, stream configs, KV buckets, delivery semantics, and the JobBus
// interface this file implements.
package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/johnnycube/cairn-provider-strava/internal/config"
	"github.com/johnnycube/cairn-provider-strava/internal/port"
)

// Bus is the NATS-backed JobBus adapter. Wraps a single *nats.Conn and
// the jetstream.JetStream context on top of it.
//
// Lifecycle:
//
//	bus, err := nats.NewBusFromConn(nc, clientName, logger)
//	defer bus.Close()
//	// pass `bus` as port.JobBus into the worker SDK
type Bus struct {
	cfg    config.NATSConfig
	logger *slog.Logger

	conn *nats.Conn
	js   jetstream.JetStream

	// kvs caches resolved KeyValue handles so repeated KV("...") calls
	// don't re-resolve every time.
	mu  sync.Mutex
	kvs map[string]jetstream.KeyValue
}

// NewBusFromConn wraps an already-connected *nats.Conn into a Bus.
// Workers build their own connection with the enrollment token + ephemeral
// nkey before the auth-callout admits them. The Bus takes ownership of the
// connection — caller should not Close it directly; instead call Bus.Close
// which drains + closes.
//
// Workers don't declare streams; the server does.
func NewBusFromConn(nc *nats.Conn, clientName string, logger *slog.Logger) (*Bus, error) {
	if logger == nil {
		logger = slog.Default()
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("nats: create jetstream context: %w", err)
	}
	return &Bus{
		cfg: config.NATSConfig{
			URL:        nc.ConnectedUrl(),
			ClientName: clientName,
			// A fetch_source job makes up to two Strava HTTP calls (30s timeout
			// each) plus rate-limit waits, so the NATS default 30s AckWait would
			// redeliver mid-processing. Give handlers room. MaxDeliver stays
			// unbounded: the handler Terms genuine poison (bad payload /
			// needs_reauth) immediately, while transient + rate-limited errors
			// should retry until they resolve rather than DLQ a real activity.
			JobAckWait: 2 * time.Minute,
		},
		logger: logger.With("component", "nats_bus"),
		conn:   nc,
		js:     js,
		kvs:    map[string]jetstream.KeyValue{},
	}, nil
}

// Close drains pending messages and tears down the connection.
// Safe to call multiple times.
func (b *Bus) Close() {
	if b.conn == nil {
		return
	}
	// Drain blocks until pending publishes flush and subscriptions
	// finish processing. Bounded by the connection drain timeout.
	if err := b.conn.Drain(); err != nil {
		b.logger.Warn("nats: drain failed", "error", err)
	}
	// After Drain, Close releases socket + goroutines.
	b.conn.Close()
	b.conn = nil
}

// ---------------------------------------------------------------------------
// port.JobBus: Publish
// ---------------------------------------------------------------------------

// Publish sends a message. If the subject is a JetStream subject (matches
// any declared stream), it's published via JetStream and durably stored.
// Otherwise it goes via core NATS (fire-and-forget).
//
// msgID populates the Nats-Msg-Id header so JetStream's per-stream
// deduplication window collapses retries to one delivery.
func (b *Bus) Publish(ctx context.Context, subject string, msgID string, body []byte) error {
	msg := &nats.Msg{
		Subject: subject,
		Data:    body,
		Header:  nats.Header{},
	}
	if msgID != "" {
		msg.Header.Set(jetstream.MsgIDHeader, msgID)
	}
	// JetStream PublishMsg routes to whichever stream owns the subject.
	// If no stream owns it, JetStream returns ErrNoStreamResponse and
	// we fall back to core NATS publish.
	_, err := b.js.PublishMsg(ctx, msg)
	if err == nil {
		return nil
	}
	if errors.Is(err, nats.ErrNoStreamResponse) || errors.Is(err, jetstream.ErrNoStreamResponse) {
		// Core NATS publish — for subjects outside any declared stream.
		if pubErr := b.conn.PublishMsg(msg); pubErr != nil {
			return fmt.Errorf("nats: core publish %s: %w", subject, pubErr)
		}
		return nil
	}
	return fmt.Errorf("nats: js publish %s: %w", subject, err)
}

// ---------------------------------------------------------------------------
// port.JobBus: Pull (pull consumer)
// ---------------------------------------------------------------------------

// Pull creates (or updates) a durable pull consumer that workers fetch
// from. The caller controls batch size and ack timing per message —
// useful for backpressure-aware workers.
func (b *Bus) Pull(
	ctx context.Context,
	cfg port.ConsumerConfig,
) (port.PullSubscription, error) {
	cons, err := b.resolveConsumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &pullSubscription{cons: cons, logger: b.logger.With("consumer", cfg.Durable)}, nil
}

// resolveConsumer calls CreateOrUpdateConsumer so configuration changes
// (AckWait, MaxDeliver, BackoffSchedule) take effect after a restart
// without manual cleanup.
func (b *Bus) resolveConsumer(
	ctx context.Context,
	cfg port.ConsumerConfig,
) (jetstream.Consumer, error) {
	if cfg.Stream == "" {
		return nil, errors.New("nats: ConsumerConfig.Stream required")
	}
	if cfg.Durable == "" {
		return nil, errors.New("nats: ConsumerConfig.Durable required")
	}

	maxDeliver := cfg.MaxDeliver
	if maxDeliver == 0 {
		maxDeliver = b.cfg.JobMaxDeliver
	}
	ackWait := cfg.AckWait
	if ackWait == 0 {
		ackWait = b.cfg.JobAckWait
	}

	deliverPolicy := jetstream.DeliverAllPolicy
	switch cfg.DeliverPolicy {
	case port.DeliverNew:
		deliverPolicy = jetstream.DeliverNewPolicy
	case port.DeliverLast:
		deliverPolicy = jetstream.DeliverLastPolicy
	case port.DeliverAll, "":
		deliverPolicy = jetstream.DeliverAllPolicy
	}

	cc := jetstream.ConsumerConfig{
		Durable:       cfg.Durable,
		FilterSubject: cfg.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxDeliver:    maxDeliver,
		DeliverPolicy: deliverPolicy,
		BackOff:       cfg.BackoffSchedule,
	}

	cons, err := b.js.CreateOrUpdateConsumer(ctx, cfg.Stream, cc)
	if err != nil {
		return nil, fmt.Errorf("nats: create/update consumer %s on %s: %w",
			cfg.Durable, cfg.Stream, err)
	}
	return cons, nil
}

// ---------------------------------------------------------------------------
// port.JobBus: Request / RespondTo (request-reply)
// ---------------------------------------------------------------------------

// Request sends a request and waits for one reply. Used for the OAuth
// token-fetch and blob-presign flows where the caller wants a synchronous
// answer. Core NATS, not JetStream — replies don't need durability.
func (b *Bus) Request(
	ctx context.Context,
	subject string,
	body []byte,
	timeout time.Duration,
) ([]byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	msg, err := b.conn.RequestWithContext(reqCtx, subject, body)
	if err != nil {
		return nil, fmt.Errorf("nats: request %s: %w", subject, err)
	}
	return msg.Data, nil
}

// RespondTo subscribes to `subject` and dispatches each incoming request
// to `handler`; the handler's return value is published as the reply.
// Errors are mapped to a NATS error header so the requester sees a
// specific failure reason.
func (b *Bus) RespondTo(
	ctx context.Context,
	subject string,
	handler port.RequestHandler,
) (port.Subscription, error) {
	sub, err := b.conn.Subscribe(subject, func(msg *nats.Msg) {
		if msg.Reply == "" {
			b.logger.Warn("nats: request without reply subject", "subject", subject)
			return
		}
		resp, herr := handler(ctx, msg.Data)
		reply := &nats.Msg{
			Subject: msg.Reply,
			Data:    resp,
			Header:  nats.Header{},
		}
		if herr != nil {
			reply.Header.Set("Nats-Error", herr.Error())
			if len(resp) == 0 {
				reply.Data = []byte(herr.Error())
			}
		}
		if pubErr := b.conn.PublishMsg(reply); pubErr != nil {
			b.logger.Warn("nats: reply publish failed", "subject", subject, "error", pubErr)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("nats: subscribe %s: %w", subject, err)
	}
	return &coreSubscription{sub: sub}, nil
}

// ---------------------------------------------------------------------------
// port.JobBus: KV handles
// ---------------------------------------------------------------------------

// KV returns a handle on the named KV bucket. The bucket must already
// exist — the cairn-core server bootstraps it at startup.
func (b *Bus) KV(bucket string) (port.KV, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if h, ok := b.kvs[bucket]; ok {
		return &kvHandle{kv: h}, nil
	}
	h, err := b.js.KeyValue(context.Background(), bucket)
	if err != nil {
		return nil, fmt.Errorf("nats: resolve kv %s: %w", bucket, err)
	}
	b.kvs[bucket] = h
	return &kvHandle{kv: h}, nil
}

type kvHandle struct {
	kv jetstream.KeyValue
}

func (h *kvHandle) Get(ctx context.Context, key string) (port.KVEntry, error) {
	entry, err := h.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return port.KVEntry{}, port.ErrKVKeyNotFound
		}
		return port.KVEntry{}, fmt.Errorf("kv get %s: %w", key, err)
	}
	return port.KVEntry{
		Key:       entry.Key(),
		Value:     entry.Value(),
		Revision:  entry.Revision(),
		CreatedAt: entry.Created(),
	}, nil
}

func (h *kvHandle) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	rev, err := h.kv.Put(ctx, key, value)
	if err != nil {
		return 0, fmt.Errorf("kv put %s: %w", key, err)
	}
	return rev, nil
}

// CompareAndSet writes if the current revision matches expectedRev. On
// mismatch returns (newRev=0, ok=false, err=nil) so the caller's retry
// loop sees a clear retry signal.
func (h *kvHandle) CompareAndSet(
	ctx context.Context,
	key string,
	value []byte,
	expectedRev uint64,
) (uint64, bool, error) {
	rev, err := h.kv.Update(ctx, key, value, expectedRev)
	if err == nil {
		return rev, true, nil
	}
	// ErrKeyExists covers the create-vs-update collision; a version
	// mismatch on update surfaces as JSAPI code 10071 ("wrong last
	// sequence"). Both are non-fatal for the CAS contract.
	if errors.Is(err, jetstream.ErrKeyExists) || isWrongLastSeqErr(err) {
		return 0, false, nil
	}
	return 0, false, fmt.Errorf("kv update %s: %w", key, err)
}

func isWrongLastSeqErr(err error) bool {
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode == 10071
}

// ---------------------------------------------------------------------------
// Subscription wrappers
// ---------------------------------------------------------------------------

type pullSubscription struct {
	cons   jetstream.Consumer
	logger *slog.Logger
}

func (s *pullSubscription) Close(_ context.Context) error { return nil }

// Fetch pulls up to `batchSize` messages, blocking briefly (FetchMaxWait)
// if fewer are available. Returns an empty slice on timeout.
func (s *pullSubscription) Fetch(ctx context.Context, batchSize int) ([]port.PullMessage, error) {
	if batchSize <= 0 {
		batchSize = 1
	}
	batch, err := s.cons.Fetch(batchSize, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("nats: fetch: %w", err)
	}

	var out []port.PullMessage
	for jm := range batch.Messages() {
		out = append(out, &pullMessage{jm: jm})
	}
	if err := batch.Error(); err != nil {
		return out, fmt.Errorf("nats: fetch batch: %w", err)
	}
	return out, nil
}

type pullMessage struct {
	jm jetstream.Msg
}

func (m *pullMessage) Message() port.Message { return wrapMessage(m.jm) }

func (m *pullMessage) Ack(_ context.Context) error  { return m.jm.Ack() }
func (m *pullMessage) Nak(_ context.Context) error  { return m.jm.Nak() }
func (m *pullMessage) Term(_ context.Context) error { return m.jm.Term() }

func (m *pullMessage) NakWithDelay(_ context.Context, delay time.Duration) error {
	return m.jm.NakWithDelay(delay)
}

func (m *pullMessage) InProgress(_ context.Context) error { return m.jm.InProgress() }

type coreSubscription struct {
	sub *nats.Subscription
}

func (s *coreSubscription) Close(_ context.Context) error {
	if s.sub != nil {
		return s.sub.Drain()
	}
	return nil
}

// wrapMessage converts a jetstream.Msg into a port.Message snapshot.
// Ack/Nak stay on PullMessage so the dispatch loop owns disposition.
func wrapMessage(jm jetstream.Msg) port.Message {
	headers := map[string]string{}
	for k, vals := range jm.Headers() {
		if len(vals) > 0 {
			headers[k] = vals[0]
		}
	}
	attempt := 0
	if meta, err := jm.Metadata(); err == nil && meta != nil {
		attempt = int(meta.NumDelivered)
	}
	return port.Message{
		Subject:         jm.Subject(),
		Headers:         headers,
		Body:            jm.Data(),
		DeliveryAttempt: attempt,
	}
}
