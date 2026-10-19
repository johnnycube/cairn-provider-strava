package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/opencairn-org/cairn-provider-strava/internal/port"
)

// startServer runs an embedded JetStream-enabled NATS server for one test.
func startServer(t *testing.T) *server.Server {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server not ready")
	}
	t.Cleanup(s.Shutdown)
	return s
}

// newTestBus connects a Bus to a fresh server with a JOBS stream and a KV bucket.
func newTestBus(t *testing.T) (*Bus, jetstream.JetStream) {
	t.Helper()
	s := startServer(t)
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	bus, err := NewBusFromConn(nc, "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bus.Close)
	ctx := context.Background()
	if _, err := bus.js.CreateStream(ctx, jetstream.StreamConfig{Name: "JOBS", Subjects: []string{"jobs.>"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "kv"}); err != nil {
		t.Fatal(err)
	}
	return bus, bus.js
}

func TestBus_PublishPullAndDisposition(t *testing.T) {
	bus, js := newTestBus(t)
	ctx := context.Background()

	for _, id := range []string{"a", "b", "c", "d", "a"} {
		if err := bus.Publish(ctx, "jobs.x", id, []byte("body-"+id)); err != nil {
			t.Fatal(err)
		}
	}
	info, err := js.Stream(ctx, "JOBS")
	if err != nil {
		t.Fatal(err)
	}
	if n := info.CachedInfo().State.Msgs; n != 4 {
		t.Fatalf("stream msgs = %d; want 4 (msg-id dedup)", n)
	}

	sub, err := bus.Pull(ctx, port.ConsumerConfig{Stream: "JOBS", Durable: "d1", Subject: "jobs.x", DeliverPolicy: port.DeliverAll})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(ctx) }()
	msgs, err := sub.Fetch(ctx, 0)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("fetch(0) = %d msgs, %v; want 1", len(msgs), err)
	}
	m := msgs[0].Message()
	if m.Subject != "jobs.x" || string(m.Body) != "body-a" || m.Headers[jetstream.MsgIDHeader] != "a" || m.DeliveryAttempt != 1 {
		t.Fatalf("message = %+v", m)
	}
	if err := msgs[0].InProgress(ctx); err != nil {
		t.Fatal(err)
	}
	if err := msgs[0].Ack(ctx); err != nil {
		t.Fatal(err)
	}

	msgs, err = sub.Fetch(ctx, 3)
	if err != nil || len(msgs) != 3 {
		t.Fatalf("fetch(3) = %d msgs, %v", len(msgs), err)
	}
	if err := msgs[0].Term(ctx); err != nil {
		t.Fatal(err)
	}
	if err := msgs[1].NakWithDelay(ctx, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := msgs[2].Nak(ctx); err != nil {
		t.Fatal(err)
	}

	msgs, err = sub.Fetch(ctx, 2)
	if err != nil || len(msgs) != 2 {
		t.Fatalf("redelivery fetch = %d msgs, %v", len(msgs), err)
	}
	for _, m := range msgs {
		if m.Message().DeliveryAttempt != 2 {
			t.Errorf("redelivered attempt = %d; want 2", m.Message().DeliveryAttempt)
		}
	}
}

func TestBus_PublishOutsideStreamFallsBackToCore(t *testing.T) {
	bus, _ := newTestBus(t)
	// No stream and no subscriber: JetStream reports no responders, core publish takes over.
	if err := bus.Publish(context.Background(), "core.subject", "", []byte("hi")); err != nil {
		t.Fatal(err)
	}
}

func TestBus_PublishToCoreSubscriberDeliversOnceWithoutWaiting(t *testing.T) {
	bus, _ := newTestBus(t)
	got := make(chan *nats.Msg, 4)
	sub, err := bus.conn.ChanSubscribe("core.listened", got)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := bus.conn.Flush(); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := bus.Publish(context.Background(), "core.listened", "m1", []byte("hi")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("publish took %v; a core subscriber must not be awaited as a JetStream ack", d)
	}
	if m := <-got; string(m.Data) != "hi" || m.Header.Get(jetstream.MsgIDHeader) != "m1" {
		t.Fatalf("message = %q %v", m.Data, m.Header)
	}
	select {
	case m := <-got:
		t.Fatalf("delivered twice: %q", m.Data)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestBus_PublishPicksUpStreamCreatedLater(t *testing.T) {
	bus, js := newTestBus(t)
	ctx := context.Background()
	bus.noStreamTTL = 0
	if err := bus.Publish(ctx, "late.x", "", []byte("core")); err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "LATE", Subjects: []string{"late.>"}}); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(ctx, "late.x", "", []byte("stored")); err != nil {
		t.Fatal(err)
	}
	info, err := js.Stream(ctx, "LATE")
	if err != nil {
		t.Fatal(err)
	}
	if n := info.CachedInfo().State.Msgs; n != 1 {
		t.Fatalf("stream msgs = %d; want 1", n)
	}
}

func TestBus_PublishFallsBackToCoreWhenStreamDeleted(t *testing.T) {
	bus, js := newTestBus(t)
	ctx := context.Background()
	if err := bus.Publish(ctx, "jobs.gone", "", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := js.DeleteStream(ctx, "JOBS"); err != nil {
		t.Fatal(err)
	}
	// Cached stream is gone and nobody listens: no responders, so core publish takes over.
	if err := bus.Publish(ctx, "jobs.gone", "", []byte("b")); err != nil {
		t.Fatal(err)
	}
}

func TestBus_PullConfigValidation(t *testing.T) {
	bus, _ := newTestBus(t)
	ctx := context.Background()
	if _, err := bus.Pull(ctx, port.ConsumerConfig{Durable: "d"}); err == nil {
		t.Error("missing stream: want error")
	}
	if _, err := bus.Pull(ctx, port.ConsumerConfig{Stream: "JOBS"}); err == nil {
		t.Error("missing durable: want error")
	}
	if _, err := bus.Pull(ctx, port.ConsumerConfig{Stream: "NOPE", Durable: "d"}); err == nil {
		t.Error("unknown stream: want error")
	}
	for _, p := range []port.DeliverPolicy{port.DeliverNew, port.DeliverLast, ""} {
		if _, err := bus.Pull(ctx, port.ConsumerConfig{Stream: "JOBS", Durable: "p" + string(p), Subject: "jobs.y", DeliverPolicy: p, AckWait: time.Second, MaxDeliver: 3}); err != nil {
			t.Errorf("policy %q: %v", p, err)
		}
	}
}

func TestBus_RequestRespond(t *testing.T) {
	bus, _ := newTestBus(t)
	ctx := context.Background()

	sub, err := bus.RespondTo(ctx, "rpc.echo", func(_ context.Context, body []byte) ([]byte, error) {
		if string(body) == "fail" {
			return nil, errors.New("boom")
		}
		return append([]byte("re:"), body...), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	out, err := bus.Request(ctx, "rpc.echo", []byte("x"), 5*time.Second)
	if err != nil || string(out) != "re:x" {
		t.Fatalf("request = %q, %v", out, err)
	}
	out, err = bus.Request(ctx, "rpc.echo", []byte("fail"), 5*time.Second)
	if err != nil || string(out) != "boom" {
		t.Fatalf("failing request = %q, %v; want the error text as body", out, err)
	}
	// A plain publish without a reply subject is ignored.
	if err := bus.conn.Publish("rpc.echo", []byte("noreply")); err != nil {
		t.Fatal(err)
	}
	if err := sub.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.Request(ctx, "rpc.echo", []byte("x"), 200*time.Millisecond); err == nil {
		t.Error("request after close: want error")
	}
	if err := (&coreSubscription{}).Close(ctx); err != nil {
		t.Error(err)
	}
}

func TestBus_KV(t *testing.T) {
	bus, _ := newTestBus(t)
	ctx := context.Background()

	if _, err := bus.KV("missing"); err == nil {
		t.Error("unknown bucket: want error")
	}
	kv, err := bus.KV("kv")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bus.KV("kv"); err != nil {
		t.Fatal("cached handle:", err)
	}
	if _, err := kv.Get(ctx, "k"); !errors.Is(err, port.ErrKVKeyNotFound) {
		t.Fatalf("get missing = %v; want ErrKVKeyNotFound", err)
	}
	if _, err := kv.Get(ctx, "bad key!"); err == nil || errors.Is(err, port.ErrKVKeyNotFound) {
		t.Errorf("invalid key get = %v; want a non-not-found error", err)
	}
	rev, err := kv.Put(ctx, "k", []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, "bad key!", nil); err == nil {
		t.Error("invalid key put: want error")
	}
	e, err := kv.Get(ctx, "k")
	if err != nil || string(e.Value) != "v1" || e.Revision != rev || e.Key != "k" {
		t.Fatalf("get = %+v, %v", e, err)
	}
	if _, ok, err := kv.CompareAndSet(ctx, "k", []byte("v2"), rev+10); ok || err != nil {
		t.Fatalf("stale CAS = %v, %v; want false, nil", ok, err)
	}
	if _, ok, err := kv.CompareAndSet(ctx, "k", []byte("v2"), 0); ok || err != nil {
		t.Fatalf("create-over-existing CAS = %v, %v; want false, nil", ok, err)
	}
	newRev, ok, err := kv.CompareAndSet(ctx, "k", []byte("v2"), rev)
	if !ok || err != nil || newRev <= rev {
		t.Fatalf("CAS = %d, %v, %v", newRev, ok, err)
	}
	if _, _, err := kv.CompareAndSet(ctx, "bad key!", nil, 1); err == nil {
		t.Error("invalid key CAS: want error")
	}
}

func TestBus_CloseIdempotent(t *testing.T) {
	bus, _ := newTestBus(t)
	bus.Close()
	bus.Close()
}

func TestRateLimiter_OnNATSKV(t *testing.T) {
	bus, _ := newTestBus(t)
	ctx := context.Background()
	kv, err := bus.KV("kv")
	if err != nil {
		t.Fatal(err)
	}
	rl := NewRateLimiter(kv, map[string]int{"strava:short": 2})
	for i := 0; i < 2; i++ {
		if ok, _, err := rl.Reserve(ctx, "strava:short", 1); !ok || err != nil {
			t.Fatalf("reserve %d = %v, %v", i, ok, err)
		}
	}
	if ok, _, err := rl.Reserve(ctx, "strava:short", 1); ok || err != nil {
		t.Fatalf("reserve over capacity = %v, %v", ok, err)
	}
}
