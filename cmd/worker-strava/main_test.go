package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/johnnycube/cairn-provider-strava/internal/inmem"
	"github.com/johnnycube/cairn-provider-strava/internal/port"
	"github.com/johnnycube/cairn-provider-strava/internal/workersdk"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestLoadConfig(t *testing.T) {
	t.Setenv("CAIRN_NATS_URL", "nats://n:4222")
	t.Setenv("CAIRN_WORKER_NAME", "")
	t.Setenv("CAIRN_WORKER_INSTANCE_ID", "i1")
	t.Setenv("CAIRN_LOG_LEVEL", "debug")
	t.Setenv("CAIRN_WORKER_ENROLLMENT_TOKEN", "tok")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NATSURL != "nats://n:4222" || cfg.WorkerName != workerName || cfg.InstanceID != "i1" || cfg.LogLevel != slog.LevelDebug || cfg.EnrollmentToken != "tok" {
		t.Errorf("cfg = %+v", cfg)
	}
	t.Setenv("CAIRN_LOG_LEVEL", "loud")
	if _, err := loadConfig(); err == nil {
		t.Error("bad log level: want error")
	}
	if hostnameOr("x") == "" {
		t.Error("hostnameOr empty")
	}
}

func TestStravaManifest_Valid(t *testing.T) {
	if err := stravaManifest().Validate(); err != nil {
		t.Fatal(err)
	}
}

// kvlessBus fails every KV lookup.
type kvlessBus struct{ *inmem.Bus }

func (kvlessBus) KV(string) (port.KV, error) { return nil, errors.New("no bucket") }

func TestNewRateLimiter(t *testing.T) {
	if newRateLimiter(inmem.New(), quietLogger()) == nil {
		t.Error("want a limiter when the bucket exists")
	}
	if newRateLimiter(kvlessBus{inmem.New()}, quietLogger()) != nil {
		t.Error("want nil without the bucket")
	}
}

func TestRegisterHandlers_SyncsUsageAndWebhooks(t *testing.T) {
	bus := inmem.New()
	kv, _ := bus.KV("cairn_rate_limits")
	w, err := workersdk.New(workersdk.Config{Name: workerName, Provider: workerProvider, Bus: bus, Limiter: newRateLimiter(bus, quietLogger())})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CAIRN_STRAVA_WEBHOOK_VERIFY_TOKEN", "s3cret")
	client := newStravaClient()
	registerHandlers(w, workerConfig{WorkerName: workerName}, client, quietLogger())

	client.onUsage(10, 100, 20, 1000)
	for _, key := range []string{"strava.short", "strava.daily"} {
		if _, err := kv.Get(context.Background(), key); err != nil {
			t.Errorf("usage not synced to %s: %v", key, err)
		}
	}
	defer func() {
		if recover() == nil {
			t.Error("webhook handlers not registered")
		}
	}()
	w.WebhookVerify(handleWebhookVerify)
}

func startNATS(t *testing.T) string {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(s.Shutdown)
	return s.ClientURL()
}

func TestConnectBus(t *testing.T) {
	url := startNATS(t)
	for name, cfg := range map[string]workerConfig{
		"anonymous":   {NATSURL: url, WorkerName: "w"},
		"static user": {NATSURL: url, WorkerName: "w", NATSUser: "u", NATSPassword: "p"},
	} {
		t.Run(name, func(t *testing.T) {
			bus, err := connectBus(cfg, quietLogger())
			if err != nil {
				t.Fatal(err)
			}
			bus.Close()
		})
	}
	if _, err := connectBus(workerConfig{NATSURL: "nats://127.0.0.1:1"}, quietLogger()); err == nil {
		t.Error("unreachable server: want error")
	}
}

// startAuthCalloutNATS runs core's auth-callout nats.conf (no nkey users) with a
// fake callout that admits token and binds the JWT to req.UserNkey, as core does.
func startAuthCalloutNATS(t *testing.T, token string) (url string, nkeyOnConnect func() []string) {
	t.Helper()
	acct, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	acctPub, _ := acct.PublicKey()
	t.Setenv("CAIRN_NATS_PASSWORD", "core-pw")
	t.Setenv("CAIRN_NATS_AUTH_ISSUER", acctPub)
	opts, err := server.ProcessConfigFile("testdata/nats-auth-callout.conf")
	if err != nil {
		t.Fatal(err)
	}
	opts.Host, opts.Port, opts.HTTPPort, opts.StoreDir = "127.0.0.1", -1, 0, t.TempDir()
	opts.NoLog, opts.NoSigs = true, true
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(s.Shutdown)

	core, err := nats.Connect(s.ClientURL(), nats.UserInfo("cairn-core", "core-pw"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(core.Close)
	var mu sync.Mutex
	var seen []string
	_, err = core.Subscribe("$SYS.REQ.USER.AUTH", func(msg *nats.Msg) {
		req, err := jwt.DecodeAuthorizationRequestClaims(string(msg.Data))
		if err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		mu.Lock()
		seen = append(seen, req.ConnectOptions.Nkey)
		mu.Unlock()
		resp := jwt.NewAuthorizationResponseClaims(req.UserNkey)
		resp.Audience = req.Server.ID
		resp.IssuerAccount = acctPub
		if req.ConnectOptions.Token == token {
			uc := jwt.NewUserClaims(req.UserNkey)
			uc.Audience = "CAIRN"
			uc.Pub.Allow.Add(">")
			uc.Sub.Allow.Add(">")
			if resp.Jwt, err = uc.Encode(acct); err != nil {
				t.Errorf("encode user jwt: %v", err)
				return
			}
		} else {
			resp.Error = "invalid_token"
		}
		enc, err := resp.Encode(acct)
		if err != nil {
			t.Errorf("encode response: %v", err)
			return
		}
		_ = msg.Respond([]byte(enc))
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := core.Flush(); err != nil {
		t.Fatal(err)
	}
	return s.ClientURL(), func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), seen...) }
}

func TestConnectBus_EnrollmentToken_AuthCallout(t *testing.T) {
	url, seen := startAuthCalloutNATS(t, "enroll-1")
	bus, err := connectBus(workerConfig{NATSURL: url, WorkerName: "w", EnrollmentToken: "enroll-1"}, quietLogger())
	if err != nil {
		t.Fatalf("token-only connect without a placeholder nkey user: %v", err)
	}
	bus.Close()
	if got := seen(); len(got) != 1 || got[0] != "" {
		t.Errorf("CONNECT nkeys = %q; want one token-only connect", got)
	}
	_, err = connectBus(workerConfig{NATSURL: url, WorkerName: "w", EnrollmentToken: "wrong"}, quietLogger())
	if err == nil || !strings.Contains(err.Error(), "Authorization Violation") {
		t.Fatalf("wrong token: err = %v; want an authorization violation", err)
	}
}

func TestStravaClient_OtherEndpoints(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/segments/7":
			_, _ = w.Write([]byte(`{"id":7,"name":"Climb","total_elevation_gain":12.5,"map":{"polyline":"abc"}}`))
		case "/activities/1/photos":
			if r.URL.Query().Get("photo_sources") != "true" || r.URL.Query().Get("size") != "2048" {
				t.Errorf("photo query = %v", r.URL.Query())
			}
			_, _ = w.Write([]byte(`[{"unique_id":"p","urls":{"2048":"u"}}]`))
		case "/athlete/activities":
			if r.URL.Query().Get("after") != "99" {
				t.Errorf("after = %q", r.URL.Query().Get("after"))
			}
			_, _ = w.Write([]byte(`[{"id":1}]`))
		case "/activities/400":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("bad param\n"))
		default:
			_, _ = w.Write([]byte("{not json"))
		}
	})
	ctx := context.Background()
	seg, err := c.GetSegment(ctx, "t", "7")
	if err != nil || seg.Map.Polyline != "abc" || seg.Name != "Climb" {
		t.Errorf("segment = %+v, %v", seg, err)
	}
	photos, err := c.GetActivityPhotos(ctx, "t", "1", 2048)
	if err != nil || len(photos) != 1 {
		t.Errorf("photos = %v, %v", photos, err)
	}
	if list, err := c.ListAthleteActivities(ctx, "t", 99, 1, 10); err != nil || len(list) != 1 {
		t.Errorf("list = %v, %v", list, err)
	}
	if _, err := c.GetActivity(ctx, "t", "400"); err == nil || err.Error() != "strava: 400 bad param" {
		t.Errorf("400 = %v", err)
	}
	for name, call := range map[string]func() error{
		"activity": func() error { _, err := c.GetActivity(ctx, "t", "x"); return err },
		"segment":  func() error { _, err := c.GetSegment(ctx, "t", "x"); return err },
		"streams":  func() error { _, err := c.GetActivityStreams(ctx, "t", "x"); return err },
		"photos":   func() error { _, err := c.GetActivityPhotos(ctx, "t", "x", 1); return err },
	} {
		if call() == nil {
			t.Errorf("%s: undecodable body must error", name)
		}
	}
	c.baseURL = "http://127.0.0.1:1"
	if _, err := c.GetSegment(ctx, "t", "7"); err == nil {
		t.Error("network error: want error")
	}
	for name, call := range map[string]func() error{
		"photos": func() error { _, err := c.GetActivityPhotos(ctx, "t", "1", 1); return err },
		"list":   func() error { _, err := c.ListAthleteActivities(ctx, "t", 0, 1, 1); return err },
		"stream": func() error { _, err := c.GetActivityStreams(ctx, "t", "1"); return err },
	} {
		if call() == nil {
			t.Errorf("%s: network error must surface", name)
		}
	}
	c.baseURL = "://bad"
	if _, err := c.GetActivity(ctx, "t", "1"); err == nil {
		t.Error("bad url: want error")
	}
}

func TestReportUsage_FallsBackToOverallHeaders(t *testing.T) {
	var got [4]int
	c := newStravaClient()
	c.onUsage = func(us, ls, ud, ld int) { got = [4]int{us, ls, ud, ld} }
	h := http.Header{}
	h.Set("X-RateLimit-Limit", "200,2000")
	h.Set("X-RateLimit-Usage", "150,900")
	c.reportUsage(h)
	if got != [4]int{150, 200, 900, 2000} {
		t.Errorf("got %v", got)
	}
	got = [4]int{}
	h.Set("X-RateLimit-Usage", "x,1")
	c.reportUsage(h)
	if got != [4]int{} {
		t.Errorf("malformed headers must be ignored, got %v", got)
	}
}

func TestClassifyRateLimit_HTTPDate(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", time.Now().Add(time.Minute).UTC().Format(http.TimeFormat))
	var rl *StravaRateLimitError
	if err := classifyRateLimit(h); !errors.As(err, &rl) || rl.RetryAfter <= 0 || rl.RetryAfter > time.Minute || rl.Error() == "" {
		t.Fatalf("err = %v", err)
	}
	h.Set("Retry-After", "garbage")
	if err := classifyRateLimit(h); !errors.As(err, &rl) || rl.RetryAfter != 0 {
		t.Fatalf("garbage retry-after = %v", err)
	}
}

func TestBlobHelpers(t *testing.T) {
	ctx := context.Background()
	var gotHeader, gotCT, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader, gotCT, gotMethod = r.Header.Get("X-Amz-Meta"), r.Header.Get("Content-Type"), r.Method
		if r.URL.Path == "/fail" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
	}))
	defer srv.Close()

	up := workersdk.PresignedURL{URL: srv.URL + "/ok", RequiredHeaders: map[string]string{"X-Amz-Meta": "m", "Content-Type": "application/x"}}
	if err := putPresigned(ctx, up, []byte("b"), "application/json"); err != nil || gotHeader != "m" || gotCT != "application/x" || gotMethod != http.MethodPut {
		t.Errorf("put = %v (header %q ct %q method %q)", err, gotHeader, gotCT, gotMethod)
	}
	if err := putPresigned(ctx, workersdk.PresignedURL{URL: srv.URL + "/fail"}, nil, "x"); err == nil {
		t.Error("403 put: want error")
	}
	if err := putPresigned(ctx, workersdk.PresignedURL{URL: "http://127.0.0.1:1"}, nil, "x"); err == nil {
		t.Error("unreachable put: want error")
	}
	if err := putPresigned(ctx, workersdk.PresignedURL{URL: "://bad"}, nil, "x"); err == nil {
		t.Error("bad url put: want error")
	}

	if _, _, err := downloadPhoto(ctx, srv.URL+"/fail"); err == nil {
		t.Error("403 photo: want error")
	}
	if _, _, err := downloadPhoto(ctx, "://bad"); err == nil {
		t.Error("bad url photo: want error")
	}
	if _, _, err := downloadPhoto(ctx, "http://127.0.0.1:1"); err == nil {
		t.Error("unreachable photo: want error")
	}
	if _, err := downloadArchivedActivity(ctx, "http://127.0.0.1:1"); err == nil {
		t.Error("unreachable archive: want error")
	}
}

func TestMirrorPhoto_Failures(t *testing.T) {
	h, store := fetchHarness(t, &stravaAPI{})
	ctx := context.Background()
	if att := mirrorPhoto(ctx, h.w, "u", "1", "http://127.0.0.1:1/p.jpg"); att != nil {
		t.Error("download failure must yield nil")
	}
	store.put("x", []byte("img"))
	store.failPut = true
	if att := mirrorPhoto(ctx, h.w, "u", "1", store.srv.URL+"/x"); att != nil {
		t.Error("upload failure must yield nil")
	}
	bare, _ := workersdk.New(workersdk.Config{Name: workerName, Provider: workerProvider, Bus: inmem.New()})
	if att := mirrorPhoto(ctx, bare, "u", "1", store.srv.URL+"/x"); att != nil {
		t.Error("presign failure must yield nil")
	}
	offline := newStravaClient()
	offline.baseURL = "http://127.0.0.1:1"
	if got := listActivityPhotos(ctx, bare, offline, "t", "strava:short", "1", nil); got != nil {
		t.Error("no photos and no activity must yield nil")
	}
}
