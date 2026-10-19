package inmem

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/opencairn-org/cairn-provider-strava/internal/port"
)

func TestBus_PublishSubscribeDedupAndClose(t *testing.T) {
	ctx := context.Background()
	b := New()
	if _, err := b.Subscribe(ctx, port.ConsumerConfig{}, nil); err == nil {
		t.Error("empty subject: want error")
	}

	var got []port.Message
	sub, err := b.Subscribe(ctx, port.ConsumerConfig{Subject: "a.*.c"}, func(_ context.Context, m port.Message) error {
		got = append(got, m)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = b.Publish(ctx, "a.b.c", "id1", []byte("1"))
	_ = b.Publish(ctx, "a.b.c", "id1", []byte("dup"))
	_ = b.Publish(ctx, "a.b.c", "", []byte("2"))
	_ = b.Publish(ctx, "a.b.d", "", []byte("miss"))
	if len(got) != 2 || string(got[0].Body) != "1" || got[0].Headers["Nats-Msg-Id"] != "id1" || string(got[1].Body) != "2" {
		t.Fatalf("got %+v", got)
	}
	_ = sub.Close(ctx)
	_ = b.Publish(ctx, "a.b.c", "", []byte("3"))
	if len(got) != 2 {
		t.Error("closed subscription still delivered")
	}
	if _, err := b.Pull(ctx, port.ConsumerConfig{}); err == nil {
		t.Error("Pull: want unsupported error")
	}
}

func TestBus_RequestRespond(t *testing.T) {
	ctx := context.Background()
	b := New()
	if _, err := b.Request(ctx, "x", nil, time.Second); err == nil {
		t.Error("no responder: want error")
	}
	sub, _ := b.RespondTo(ctx, "x", func(_ context.Context, body []byte) ([]byte, error) {
		return append(body, '!'), nil
	})
	out, err := b.Request(ctx, "x", []byte("hi"), time.Second)
	if err != nil || string(out) != "hi!" {
		t.Fatalf("request = %q, %v", out, err)
	}
	_ = sub.Close(ctx)
	if _, err := b.Request(ctx, "x", nil, time.Second); err == nil {
		t.Error("closed responder: want error")
	}
}

func TestKV(t *testing.T) {
	ctx := context.Background()
	b := New()
	kv, _ := b.KV("bucket")
	again, _ := b.KV("bucket")
	if kv != again {
		t.Error("KV handle not cached")
	}
	if _, err := kv.Get(ctx, "k"); !errors.Is(err, port.ErrKVKeyNotFound) {
		t.Fatalf("missing get = %v", err)
	}
	if _, ok, _ := kv.CompareAndSet(ctx, "k", []byte("v"), 3); ok {
		t.Error("CAS on missing key with rev must fail")
	}
	rev, ok, _ := kv.CompareAndSet(ctx, "k", []byte("v0"), 0)
	if !ok {
		t.Fatal("create CAS failed")
	}
	if _, ok, _ := kv.CompareAndSet(ctx, "k", []byte("v"), rev+1); ok {
		t.Error("stale CAS must fail")
	}
	if _, ok, _ := kv.CompareAndSet(ctx, "k", []byte("v1"), rev); !ok {
		t.Error("CAS with current rev must succeed")
	}
	rev, _ = kv.Put(ctx, "k", []byte("v2"))
	e, err := kv.Get(ctx, "k")
	if err != nil || string(e.Value) != "v2" || e.Revision != rev || e.Key != "k" {
		t.Fatalf("get = %+v, %v", e, err)
	}
}

func TestSubjectMatches(t *testing.T) {
	cases := []struct {
		pattern, subject string
		want             bool
	}{
		{"a.b", "a.b", true},
		{"a.*", "a.b", true},
		{"a.*", "a.b.c", false},
		{"a.>", "a.b.c", true},
		{"a.>", "a", false},
		{"a.b.c", "a.b", false},
		{"a.x", "a.b", false},
	}
	for _, c := range cases {
		if got := subjectMatches(c.pattern, c.subject); got != c.want {
			t.Errorf("subjectMatches(%q, %q) = %v", c.pattern, c.subject, got)
		}
	}
}
