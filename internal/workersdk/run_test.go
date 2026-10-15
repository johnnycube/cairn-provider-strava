package workersdk

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johnnycube/cairn-provider-strava/internal/inmem"
	"github.com/johnnycube/cairn-provider-strava/internal/port"
)

// fakeMsg records how the worker disposed of it.
type fakeMsg struct {
	msg port.Message
	err error // returned by every disposition call

	mu          sync.Mutex
	disposition string
	delay       time.Duration
}

func (m *fakeMsg) Message() port.Message { return m.msg }

func (m *fakeMsg) set(d string, delay time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disposition, m.delay = d, delay
	return m.err
}

func (m *fakeMsg) Ack(context.Context) error  { return m.set("ack", 0) }
func (m *fakeMsg) Nak(context.Context) error  { return m.set("nak", 0) }
func (m *fakeMsg) Term(context.Context) error { return m.set("term", 0) }
func (m *fakeMsg) NakWithDelay(_ context.Context, d time.Duration) error {
	return m.set("nak_delay", d)
}
func (m *fakeMsg) InProgress(context.Context) error { return nil }

func (m *fakeMsg) state() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.disposition
}

// fakeSub hands out queued batches, then idles until ctx ends.
type fakeSub struct {
	mu      sync.Mutex
	batches [][]port.PullMessage
	errs    []error
	closed  bool
}

func (s *fakeSub) Fetch(ctx context.Context, _ int) ([]port.PullMessage, error) {
	s.mu.Lock()
	if len(s.errs) > 0 {
		err := s.errs[0]
		s.errs = s.errs[1:]
		s.mu.Unlock()
		return nil, err
	}
	if len(s.batches) > 0 {
		b := s.batches[0]
		s.batches = s.batches[1:]
		s.mu.Unlock()
		return b, nil
	}
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Millisecond):
		return nil, nil
	}
}

func (s *fakeSub) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// pullBus is an inmem bus whose Pull returns scripted subscriptions.
type pullBus struct {
	*inmem.Bus
	mu      sync.Mutex
	subs    map[string]*fakeSub
	pullErr error
	cfgs    []port.ConsumerConfig
}

func newPullBus() *pullBus { return &pullBus{Bus: inmem.New(), subs: map[string]*fakeSub{}} }

func (b *pullBus) sub(subject string) *fakeSub {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.subs[subject]
	if !ok {
		s = &fakeSub{}
		b.subs[subject] = s
	}
	return s
}

func (b *pullBus) Pull(_ context.Context, cfg port.ConsumerConfig) (port.PullSubscription, error) {
	b.mu.Lock()
	b.cfgs = append(b.cfgs, cfg)
	err := b.pullErr
	b.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return b.sub(cfg.Subject), nil
}

// waitFor polls cond until it holds or the test times out.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestRun_RequiresHandlers(t *testing.T) {
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: newPullBus()})
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("want error without handlers")
	}
}

func TestRun_DispatchesJobsAndWebhooks(t *testing.T) {
	bus := newPullBus()
	const subj = "cairn.jobs.fetch_source.p"
	ok := &fakeMsg{msg: port.Message{Subject: subj, Body: []byte("ok"), DeliveryAttempt: 1}}
	term := &fakeMsg{msg: port.Message{Subject: subj, Body: []byte("term")}, err: errors.New("term failed")}
	nak := &fakeMsg{msg: port.Message{Subject: subj, Body: []byte("nak")}}
	delayed := &fakeMsg{msg: port.Message{Subject: subj, Body: []byte("delay")}}
	bus.sub(subj).errs = []error{errors.New("transient fetch")}
	bus.sub(subj).batches = [][]port.PullMessage{{ok, term, nak, delayed}}

	hook := &fakeMsg{msg: port.Message{Subject: "cairn.webhooks.n-p.event", Body: []byte("ev"), DeliveryAttempt: 1}}
	bus.sub("cairn.webhooks.n-p.event").errs = []error{errors.New("transient webhook fetch")}
	bus.sub("cairn.webhooks.n-p.event").batches = [][]port.PullMessage{{hook}}

	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus, HeartbeatInterval: 5 * time.Millisecond})
	var seen sync.Map
	w.Handle(subj, func(_ context.Context, _ *Worker, job Job) error {
		seen.Store(string(job.Body), job.DeliveryAttempt)
		switch string(job.Body) {
		case "term":
			return &port.TerminalError{Reason: "bad"}
		case "nak":
			return errors.New("retry me")
		case "delay":
			return &port.NakWithDelayError{Reason: "rate_limited", Delay: 10 * time.Millisecond}
		}
		return nil
	})
	var gotEvent WebhookEvent
	var evMu sync.Mutex
	w.WebhookEvent(func(_ context.Context, _ *Worker, ev WebhookEvent) error {
		evMu.Lock()
		defer evMu.Unlock()
		gotEvent = ev
		return nil
	})
	w.WebhookVerify(func(_ context.Context, _ *Worker, req WebhookVerifyRequest) (WebhookVerifyResponse, error) {
		if req.Query["fail"] != "" {
			return WebhookVerifyResponse{}, errors.New("verify failed")
		}
		return WebhookVerifyResponse{Status: 200, Body: []byte(req.Query["hub.challenge"])}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	waitFor(t, "dispositions", func() bool {
		return ok.state() == "ack" && term.state() == "term" && nak.state() == "nak" && delayed.state() == "nak_delay" && hook.state() == "ack"
	})
	waitFor(t, "verify responder", func() bool {
		_, err := bus.Request(ctx, "cairn.webhooks.n-p.verify", []byte(`{}`), time.Second)
		return err == nil
	})
	out, err := bus.Request(ctx, "cairn.webhooks.n-p.verify", []byte(`{"Method":"GET","Query":{"hub.challenge":"c1"}}`), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp WebhookVerifyResponse
	if err := json.Unmarshal(out, &resp); err != nil || resp.Status != 200 || string(resp.Body) != "c1" {
		t.Fatalf("verify resp = %+v, %v", resp, err)
	}
	if _, err := bus.Request(ctx, "cairn.webhooks.n-p.verify", []byte(`{"Query":{"fail":"1"}}`), time.Second); err == nil {
		t.Error("verify handler error must surface")
	}
	if _, err := bus.Request(ctx, "cairn.webhooks.n-p.verify", []byte(`not json`), time.Second); err == nil {
		t.Error("undecodable verify request must error")
	}

	kv, _ := bus.KV(kvWorkerManifests)
	if _, err := kv.Get(ctx, "n"); err != nil {
		t.Errorf("manifest not published: %v", err)
	}
	presence, _ := bus.KV(kvWorkerPresence)
	waitFor(t, "heartbeats", func() bool {
		e, err := presence.Get(ctx, "n."+w.cfg.InstanceID)
		return err == nil && e.Revision > 2
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	evMu.Lock()
	if string(gotEvent.Body) != "ev" || gotEvent.DeliveryAttempt != 1 {
		t.Errorf("webhook event = %+v", gotEvent)
	}
	evMu.Unlock()
	if !bus.sub(subj).closed {
		t.Error("pull subscription not closed on shutdown")
	}
	if c := bus.cfgs[0]; c.Stream != "CAIRN_JOBS" || c.Durable != "n__cairn_jobs_fetch_source_p" {
		t.Errorf("consumer config = %+v", c)
	}
}

func TestRun_PullErrors(t *testing.T) {
	bus := newPullBus()
	bus.pullErr = errors.New("no stream")
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus})
	w.Handle("cairn.jobs.x", func(context.Context, *Worker, Job) error { return nil })
	if err := w.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "subscribe cairn.jobs.x") {
		t.Fatalf("Run = %v", err)
	}

	w = mustNew(t, Config{Name: "n", Provider: "p", Bus: bus})
	w.WebhookEvent(func(context.Context, *Worker, WebhookEvent) error { return nil })
	if err := w.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "webhook events") {
		t.Fatalf("Run = %v", err)
	}
}

func TestRun_StopEndsLoops(t *testing.T) {
	bus := newPullBus()
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus})
	w.Handle("cairn.jobs.x", func(context.Context, *Worker, Job) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := bus.sub("cairn.jobs.x")
	w.wg.Add(1)
	go w.pullLoop(ctx, sub, "cairn.jobs.x", nil)
	w.Stop()
	w.Stop()
	w.wg.Wait()
	if w.pause(ctx, time.Hour) {
		t.Error("pause must return false once stopped")
	}
}

func TestPause(t *testing.T) {
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: newPullBus()})
	if !w.pause(context.Background(), time.Millisecond) {
		t.Error("pause should complete")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if w.pause(ctx, time.Hour) {
		t.Error("pause must return false on cancelled ctx")
	}
}

func TestDuplicateRegistrationPanics(t *testing.T) {
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: newPullBus()})
	w.Handle("s", nil)
	w.WebhookEvent(func(context.Context, *Worker, WebhookEvent) error { return nil })
	w.WebhookVerify(func(context.Context, *Worker, WebhookVerifyRequest) (WebhookVerifyResponse, error) {
		return WebhookVerifyResponse{}, nil
	})
	for name, f := range map[string]func(){
		"handle": func() { w.Handle("s", nil) },
		"event":  func() { w.WebhookEvent(nil) },
		"verify": func() { w.WebhookVerify(nil) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: want panic", name)
				}
			}()
			f()
		}()
	}
}

func TestAckByError_DispositionFailuresAreLogged(t *testing.T) {
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: newPullBus()})
	ctx := context.Background()
	failing := func() *fakeMsg { return &fakeMsg{err: errors.New("gone")} }
	cases := map[string]error{
		"ack":       nil,
		"nak":       errors.New("x"),
		"nak_delay": &port.NakWithDelayError{Delay: time.Second},
		"term":      &port.TerminalError{Reason: "r"},
	}
	for want, err := range cases {
		m := failing()
		d := w.ackByError(ctx, m, "k", 1, err)
		if m.state() != want {
			t.Errorf("%v -> %s; want %s", err, m.state(), want)
		}
		if want == "nak_delay" && d != time.Second {
			t.Errorf("delay = %v", d)
		}
	}
}

func TestNew_Validation(t *testing.T) {
	bus := newPullBus()
	for _, cfg := range []Config{{Provider: "p", Bus: bus}, {Name: "n", Bus: bus}, {Name: "n", Provider: "p"}} {
		if _, err := New(cfg); err == nil {
			t.Errorf("New(%+v): want error", cfg)
		}
	}
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus})
	if w.cfg.PullBatchSize != 4 || w.cfg.JobStream != "CAIRN_JOBS" || w.cfg.InstanceID == "" || w.Logger() == nil {
		t.Errorf("defaults not applied: %+v", w.cfg)
	}
	if w.WorkerKey() != "n-p" || (&Worker{cfg: Config{Provider: "p"}}).WorkerKey() != "p" {
		t.Error("WorkerKey wrong")
	}
	if got := consumerName("w", "a.*.b.>"); got != "w__a__b_" {
		t.Errorf("consumerName = %q", got)
	}
	if len(randID()) != 16 {
		t.Error("randID length")
	}
}

// recLimiter records limiter calls and returns scripted results.
type recLimiter struct {
	stubLimiter
	err    error
	synced int
}

func (r *recLimiter) Reserve(context.Context, string, int) (bool, time.Duration, error) {
	return r.Allow, 0, r.err
}

func (r *recLimiter) SyncUsage(context.Context, string, int, int, time.Time) error {
	r.synced++
	return nil
}

func TestLimiterHelpers(t *testing.T) {
	ctx := context.Background()
	bare := mustNew(t, Config{Name: "n", Provider: "p", Bus: newPullBus()})
	if bare.ReserveAPI(ctx, "b", 1) != nil || bare.ObserveRateLimit429(ctx, "b", 0, time.Now()) != nil || bare.ObserveUsage(ctx, "b", 1, 2, time.Now()) != nil {
		t.Error("nil limiter must be a no-op")
	}
	lim := &recLimiter{err: errors.New("kv down")}
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: newPullBus(), Limiter: lim})
	if err := w.ReserveAPI(ctx, "b", 1); err == nil {
		t.Error("limiter error must surface")
	}
	if err := w.ObserveRateLimit429(ctx, "b", 0, time.Now()); err != nil || !lim.Refilled {
		t.Error("ObserveRateLimit429 must force-refill")
	}
	if err := w.ObserveUsage(ctx, "b", 1, 2, time.Now()); err != nil || lim.synced != 1 {
		t.Error("ObserveUsage must sync")
	}
}

func TestBusHelpers(t *testing.T) {
	ctx := context.Background()
	bus := newPullBus()
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus})

	var got []port.Message
	_, _ = bus.Subscribe(ctx, port.ConsumerConfig{Subject: ">"}, func(_ context.Context, m port.Message) error {
		got = append(got, m)
		return nil
	})
	_ = w.Enqueue(ctx, "cairn.jobs.y", "id1", []byte("e"))
	_ = w.PublishEvent(ctx, "thing.happened", "id2", []byte("ev"))
	_ = w.PublishResult(ctx, Job{Subject: "cairn.jobs.z", Headers: map[string]string{}}, "id3", []byte("r"))
	_ = w.PublishResult(ctx, Job{Subject: "cairn.jobs.z", Headers: map[string]string{"Cairn-Reply-To": "reply.here"}}, "id4", []byte("r"))
	want := []string{"cairn.jobs.y", "cairn.events.thing.happened", "cairn.results.z", "reply.here"}
	if len(got) != len(want) {
		t.Fatalf("published %d; want %d", len(got), len(want))
	}
	for i, s := range want {
		if got[i].Subject != s {
			t.Errorf("publish %d subject = %q; want %q", i, got[i].Subject, s)
		}
	}

	_, _ = bus.RespondTo(ctx, "rpc", func(_ context.Context, b []byte) ([]byte, error) { return b, nil })
	if out, err := w.Request(ctx, "rpc", []byte("x"), time.Second); err != nil || string(out) != "x" {
		t.Errorf("Request = %q, %v", out, err)
	}
}

func TestPresign(t *testing.T) {
	ctx := context.Background()
	bus := newPullBus()
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus})

	if _, err := w.PresignUpload(ctx, PresignUploadRequest{}); err == nil {
		t.Error("upload without responder: want error")
	}
	if _, err := w.PresignDownload(ctx, PresignDownloadRequest{}); err == nil {
		t.Error("download without responder: want error")
	}

	var upReq PresignUploadRequest
	_, _ = bus.RespondTo(ctx, "cairn.blobs.presign_upload.p", func(_ context.Context, b []byte) ([]byte, error) {
		_ = json.Unmarshal(b, &upReq)
		return json.Marshal(PresignedURL{URL: "http://up", BlobID: "b1"})
	})
	_, _ = bus.RespondTo(ctx, "cairn.blobs.presign_download.p", func(context.Context, []byte) ([]byte, error) {
		return []byte("not json"), nil
	})
	up, err := w.PresignUpload(ctx, PresignUploadRequest{ContentType: "x"})
	if err != nil || up.BlobID != "b1" || upReq.Provider != "p" {
		t.Fatalf("upload = %+v, %v (req %+v)", up, err, upReq)
	}
	if _, err := w.PresignDownload(ctx, PresignDownloadRequest{BlobID: "b1"}); err == nil {
		t.Error("undecodable download reply: want error")
	}

	_, _ = bus.RespondTo(ctx, "cairn.blobs.presign_upload.p", func(context.Context, []byte) ([]byte, error) {
		return []byte("not json"), nil
	})
	_, _ = bus.RespondTo(ctx, "cairn.blobs.presign_download.p", func(context.Context, []byte) ([]byte, error) {
		return json.Marshal(PresignedURL{URL: "http://down"})
	})
	if _, err := w.PresignUpload(ctx, PresignUploadRequest{}); err == nil {
		t.Error("undecodable upload reply: want error")
	}
	if down, err := w.PresignDownload(ctx, PresignDownloadRequest{BlobID: "b1"}); err != nil || down.URL != "http://down" {
		t.Errorf("download = %+v, %v", down, err)
	}
}

// kvlessBus fails every KV lookup.
type kvlessBus struct{ *pullBus }

func (kvlessBus) KV(string) (port.KV, error) { return nil, errors.New("no kv") }

func TestHeartbeatAndManifest_KVUnavailable(t *testing.T) {
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: kvlessBus{newPullBus()}})
	ctx := context.Background()
	if w.publishHeartbeat(ctx) == nil || w.publishManifest(ctx) == nil {
		t.Error("KV failure must surface")
	}
}
