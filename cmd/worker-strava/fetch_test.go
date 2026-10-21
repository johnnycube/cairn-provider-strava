package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/opencairn-org/cairn-provider-strava/internal/inmem"
	natsadapter "github.com/opencairn-org/cairn-provider-strava/internal/nats"
	"github.com/opencairn-org/cairn-provider-strava/internal/port"
	"github.com/opencairn-org/cairn-provider-strava/internal/workersdk"
	cairnv1 "github.com/opencairn-org/cairn-provider-strava/proto/cairn/v1"
	workerv1 "github.com/opencairn-org/cairn-provider-strava/proto/cairn/worker/v1"
)

// blobStore is a fake object store behind the presign responders.
type blobStore struct {
	srv *httptest.Server

	mu      sync.Mutex
	objects map[string][]byte // by blob id
	nextID  int
	failPut bool
}

// withBlobStore wires presign_upload/presign_download responders on bus to a fake store.
func withBlobStore(t *testing.T, bus *inmem.Bus) *blobStore {
	t.Helper()
	s := &blobStore{objects: map[string][]byte{}}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/")
		s.mu.Lock()
		defer s.mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			if s.failPut {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			s.objects[id], _ = io.ReadAll(r.Body)
		case http.MethodGet:
			b, ok := s.objects[id]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(b)
		}
	}))
	t.Cleanup(s.srv.Close)
	ctx := context.Background()
	if _, err := bus.RespondTo(ctx, "cairn.blobs.presign_upload.strava", func(context.Context, []byte) ([]byte, error) {
		s.mu.Lock()
		s.nextID++
		id := fmt.Sprintf("blob%d", s.nextID)
		s.mu.Unlock()
		return json.Marshal(workersdk.PresignedURL{Method: http.MethodPut, URL: s.srv.URL + "/" + id, BlobID: id})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.RespondTo(ctx, "cairn.blobs.presign_download.strava", func(_ context.Context, body []byte) ([]byte, error) {
		var req workersdk.PresignDownloadRequest
		_ = json.Unmarshal(body, &req)
		return json.Marshal(workersdk.PresignedURL{URL: s.srv.URL + "/" + req.BlobID})
	}); err != nil {
		t.Fatal(err)
	}
	return s
}

func (s *blobStore) put(id string, b []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[id] = b
}

// results decodes the claim-checked JobResults published on subject.
func (s *blobStore) results(t *testing.T, h *harness, subject string) []*workerv1.JobResult {
	t.Helper()
	var out []*workerv1.JobResult
	for _, m := range h.published(subject) {
		var env workerv1.JobResult
		if err := protojson.Unmarshal(m.Body, &env); err != nil {
			t.Fatal(err)
		}
		if env.GetPayloadRef() == nil {
			out = append(out, &env)
			continue
		}
		s.mu.Lock()
		body := s.objects[env.GetPayloadRef().GetBlobId()]
		s.mu.Unlock()
		var full workerv1.JobResult
		if err := protojson.Unmarshal(body, &full); err != nil {
			t.Fatal(err)
		}
		out = append(out, &full)
	}
	return out
}

// stravaAPI is a fake Strava API for one activity (id 42).
type stravaAPI struct {
	activity, streams, photos, segment7, athlete http.HandlerFunc
	photoURL                                     string
}

func (a *stravaAPI) handler(w http.ResponseWriter, r *http.Request) {
	serve := func(h http.HandlerFunc, body string) {
		if h != nil {
			h(w, r)
			return
		}
		_, _ = io.WriteString(w, body)
	}
	switch r.URL.Path {
	case "/activities/42":
		serve(a.activity, `{"id":42,"name":"Morning Ride","sport_type":"Ride","start_date":"2026-05-01T07:00:00Z","elapsed_time":3600,
			"gear_id":"b1","gear":{"id":"b1","primary":true,"name":"Canyon","distance":1500.5},
			"segment_efforts":[{"id":1,"segment":{"id":7}},{"id":2,"segment":{"id":7}},{"id":3,"segment":{"id":8}},{"id":4,"segment":{"id":9}}],
			"photos":{"primary":{"unique_id":"p1","urls":{"600":"`+a.photoURL+`"}}}}`)
	case "/athlete":
		serve(a.athlete, `{"id":1,"bikes":[{"id":"b1","primary":true,"name":"Canyon","distance":1500.5},{"id":"b2","name":"Old","retired":true}],
			"shoes":[{"id":"g1","primary":true,"nickname":"Pegasus"}]}`)
	case "/gear/b1":
		_, _ = io.WriteString(w, `{"id":"b1","name":"Canyon","brand_name":"Canyon","model_name":"Grail","description":"gravel","distance":1500.5}`)
	case "/gear/b2":
		_, _ = io.WriteString(w, `{"id":"b2","name":"Old","retired":true}`)
	case "/gear/g1":
		w.WriteHeader(http.StatusNotFound)
	case "/activities/42/streams":
		serve(a.streams, `{"time":{"data":[0,1,2]},"heartrate":{"data":[100,110,120]}}`)
	case "/activities/42/photos":
		serve(a.photos, `[{"unique_id":"p1","caption":"view","urls":{"2048":"`+a.photoURL+`"},"sizes":{"2048":[2048,1536]}}]`)
	case "/segments/7":
		serve(a.segment7, `{"id":7,"name":"Climb","activity_type":"Ride","climb_category":3,"map":{"polyline":"abc"}}`)
	case "/segments/8":
		_, _ = io.WriteString(w, `{"id":8,"name":"No geometry"}`)
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// fetchHarness builds a harness plus blob store and a photo server.
func fetchHarness(t *testing.T, api *stravaAPI) (*harness, *blobStore) {
	t.Helper()
	photo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("\xff\xd8\xff\xe0 jpeg bytes"))
	}))
	t.Cleanup(photo.Close)
	api.photoURL = photo.URL + "/p.jpg"
	h := newHarness(t, api.handler)
	return h, withBlobStore(t, h.bus)
}

func fetchJob(in fetchSourceJob) workersdk.Job {
	body, _ := json.Marshal(in)
	return workersdk.Job{Subject: subjFetchSource, Body: body, DeliveryAttempt: 1}
}

const resultsFetch = "cairn.results.fetch_source.strava"

// activityEvent returns the result's ACTIVITY event (gear events precede it).
func activityEvent(t *testing.T, res *workerv1.JobResult) *workerv1.ImportedActivity {
	t.Helper()
	for _, e := range res.GetEvents() {
		if a := e.GetActivity(); a != nil {
			return a
		}
	}
	t.Fatalf("no activity event in %v", res.GetEvents())
	return nil
}

func TestFetchSource_PublishesActivityAndSegments(t *testing.T) {
	h, store := fetchHarness(t, &stravaAPI{})
	handler := makeFetchSourceHandler(workerConfig{WorkerName: workerName}, h.client)
	in := fetchSourceJob{JobID: "j1", AccountID: "acct", UserID: "u1", ExtID: "42", FetchStreams: true, Reason: "webhook"}
	if err := handler(context.Background(), h.w, fetchJob(in)); err != nil {
		t.Fatalf("fetch_source: %v", err)
	}

	results := store.results(t, h, resultsFetch)
	if len(results) != 2 {
		t.Fatalf("published %d results; want activity + segments", len(results))
	}
	// The gear event precedes the activity that references it.
	evs := results[0].GetEvents()
	if len(evs) != 2 || evs[0].GetGear() == nil {
		t.Fatalf("events = %v; want GEAR then ACTIVITY", evs)
	}
	if g := evs[0].GetGear(); g.GetRef().GetExternalId() != "b1" || g.GetPayload().GetKind() != cairnv1.GearKind_GEAR_KIND_BIKE ||
		g.GetPayload().GetName() != "Canyon" || !g.GetPayload().GetPrimary() || g.GetPayload().GetDistanceM() != 1500.5 {
		t.Errorf("gear event = %v", g)
	}
	act := evs[1].GetActivity()
	if act.GetRef().GetExternalId() != "42" || act.GetPayload().GetTitle() != "Morning Ride" {
		t.Errorf("activity = %v", act.GetRef())
	}
	if act.GetPayload().GetGearExternalId() != "b1" || act.GetPayload().GetGearName() != "Canyon" {
		t.Errorf("payload gear = %q/%q", act.GetPayload().GetGearExternalId(), act.GetPayload().GetGearName())
	}
	if act.GetRawBlobId() == "" || act.GetRawContentType() != "application/json" || act.GetRawSizeBytes() == 0 {
		t.Errorf("archive not recorded: %q", act.GetRawBlobId())
	}
	if act.GetStream() == nil {
		t.Error("stream missing")
	}
	if atts := act.GetAttachments(); len(atts) != 1 || atts[0].GetCaption() != "view" || atts[0].GetWidth() != 2048 || atts[0].GetContentType() != "image/jpeg" {
		t.Errorf("attachments = %v", atts)
	}

	segEvents := results[1].GetEvents()
	var segs, efforts int
	for _, e := range segEvents {
		switch {
		case e.GetSegment() != nil:
			segs++
		case e.GetSegmentEffort() != nil:
			efforts++
		}
	}
	// Segment 7 is emitted once for two efforts; 8 lacks a polyline and 9 errors.
	if segs != 1 || efforts != 2 {
		t.Errorf("segments=%d efforts=%d; want 1 and 2", segs, efforts)
	}

	// A second job reuses the cached segment.
	if err := handler(context.Background(), h.w, fetchJob(fetchSourceJob{JobID: "j2", ExtID: "42", FetchStreams: true})); err != nil {
		t.Fatal(err)
	}
}

func TestFetchSource_PayloadValidation(t *testing.T) {
	h, _ := fetchHarness(t, &stravaAPI{})
	handler := makeFetchSourceHandler(workerConfig{}, h.client)
	var term *port.TerminalError
	if err := handler(context.Background(), h.w, workersdk.Job{Body: []byte("{")}); !errors.As(err, &term) || term.Reason != "bad_payload" {
		t.Errorf("bad payload = %v", err)
	}
	if err := handler(context.Background(), h.w, fetchJob(fetchSourceJob{})); !errors.As(err, &term) || term.Reason != "missing_ext_id" {
		t.Errorf("missing ext id = %v", err)
	}
}

func status(code int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }
}

func TestFetchSource_ActivityErrors(t *testing.T) {
	in := fetchSourceJob{JobID: "j1", AccountID: "acct", UserID: "u1", ExtID: "42"}

	t.Run("unauthorized is terminal and reports failure", func(t *testing.T) {
		h, _ := fetchHarness(t, &stravaAPI{activity: status(http.StatusUnauthorized)})
		err := makeFetchSourceHandler(workerConfig{}, h.client)(context.Background(), h.w, fetchJob(in))
		var term *port.TerminalError
		if !errors.As(err, &term) || term.Reason != "needs_reauth" {
			t.Fatalf("err = %v", err)
		}
		if n := len(h.published(resultsFetch)); n != 1 {
			t.Errorf("failure results = %d; want 1", n)
		}
	})

	t.Run("not found acks and announces deletion", func(t *testing.T) {
		h, _ := fetchHarness(t, &stravaAPI{activity: status(http.StatusNotFound)})
		if err := makeFetchSourceHandler(workerConfig{}, h.client)(context.Background(), h.w, fetchJob(in)); err != nil {
			t.Fatalf("err = %v; want nil", err)
		}
		if n := len(h.published("cairn.events.source.deleted_upstream")); n != 1 {
			t.Errorf("deleted_upstream events = %d", n)
		}
		if n := len(h.published(resultsFetch)); n != 1 {
			t.Errorf("failure results = %d", n)
		}
	})

	t.Run("rate limit naks with delay", func(t *testing.T) {
		h, _ := fetchHarness(t, &stravaAPI{activity: func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(http.StatusTooManyRequests)
		}})
		err := makeFetchSourceHandler(workerConfig{}, h.client)(context.Background(), h.w, fetchJob(in))
		var nak *port.NakWithDelayError
		if !errors.As(err, &nak) || nak.Delay < 29*time.Second || nak.Delay > 31*time.Second {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("server error is retryable", func(t *testing.T) {
		h, _ := fetchHarness(t, &stravaAPI{activity: status(http.StatusBadGateway)})
		err := makeFetchSourceHandler(workerConfig{}, h.client)(context.Background(), h.w, fetchJob(in))
		var term *port.TerminalError
		var nak *port.NakWithDelayError
		if err == nil || errors.As(err, &term) || errors.As(err, &nak) {
			t.Fatalf("err = %v; want plain error", err)
		}
	})

	t.Run("token failure passes through", func(t *testing.T) {
		h, _ := fetchHarness(t, &stravaAPI{})
		_, _ = h.bus.RespondTo(context.Background(), "cairn.tokens.strava.get", func(context.Context, []byte) ([]byte, error) {
			return json.Marshal(map[string]string{"error": "needs_reauth"})
		})
		err := makeFetchSourceHandler(workerConfig{}, h.client)(context.Background(), h.w, fetchJob(in))
		var term *port.TerminalError
		if !errors.As(err, &term) || term.Reason != "needs_reauth" {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestFetchSource_StreamOutcomes(t *testing.T) {
	in := fetchSourceJob{JobID: "j1", ExtID: "42", FetchStreams: true}

	h, store := fetchHarness(t, &stravaAPI{streams: status(http.StatusNotFound), photos: status(http.StatusInternalServerError)})
	if err := makeFetchSourceHandler(workerConfig{}, h.client)(context.Background(), h.w, fetchJob(in)); err != nil {
		t.Fatalf("streams 404: %v", err)
	}
	act := activityEvent(t, store.results(t, h, resultsFetch)[0])
	if act.GetStream() != nil {
		t.Error("stream must be empty when Strava has none")
	}
	if len(act.GetAttachments()) != 1 {
		t.Errorf("primary photo fallback: attachments = %d", len(act.GetAttachments()))
	}

	h, _ = fetchHarness(t, &stravaAPI{streams: status(http.StatusInternalServerError)})
	if err := makeFetchSourceHandler(workerConfig{}, h.client)(context.Background(), h.w, fetchJob(in)); err == nil {
		t.Fatal("streams 500: want error")
	}
}

func TestFetchSource_SegmentRateLimitDefersRest(t *testing.T) {
	h, store := fetchHarness(t, &stravaAPI{segment7: status(http.StatusTooManyRequests)})
	err := makeFetchSourceHandler(workerConfig{}, h.client)(context.Background(), h.w, fetchJob(fetchSourceJob{JobID: "j", ExtID: "42", FetchStreams: true}))
	var nak *port.NakWithDelayError
	if !errors.As(err, &nak) || nak.Reason != "rate_limited_segments" || nak.Delay <= 0 {
		t.Fatalf("err = %v", err)
	}
	if n := len(store.results(t, h, resultsFetch)); n != 1 {
		t.Errorf("results = %d; want the activity only", n)
	}
}

func TestFetchSource_BlobStoreDown(t *testing.T) {
	h, store := fetchHarness(t, &stravaAPI{})
	store.failPut = true
	err := makeFetchSourceHandler(workerConfig{}, h.client)(context.Background(), h.w, fetchJob(fetchSourceJob{JobID: "j", ExtID: "42"}))
	if err == nil || !strings.Contains(err.Error(), "upload result payload") {
		t.Fatalf("err = %v", err)
	}
}

// limitedHarness is a fetch harness whose worker reserves against real buckets.
func limitedHarness(t *testing.T, api *stravaAPI, capacities map[string]int) (*harness, *blobStore) {
	t.Helper()
	h, store := fetchHarness(t, api)
	kv, _ := h.bus.KV("cairn_rate_limits")
	w, err := workersdk.New(workersdk.Config{Name: workerName, Provider: workerProvider, Bus: h.bus, Limiter: natsadapter.NewRateLimiter(kv, capacities)})
	if err != nil {
		t.Fatal(err)
	}
	h.w = w
	return h, store
}

func TestFetchSource_BudgetExhaustion(t *testing.T) {
	in := fetchSourceJob{JobID: "j", ExtID: "42", FetchStreams: true}
	var nak *port.NakWithDelayError

	h2, _ := limitedHarness(t, &stravaAPI{}, map[string]int{"strava:short": 1})
	if err := makeFetchSourceHandler(workerConfig{}, h2.client)(context.Background(), h2.w, fetchJob(in)); !errors.As(err, &nak) {
		t.Errorf("streams reserve: err = %v", err)
	}

	// Activity, streams and photos fit; the first segment does not.
	h3, store := limitedHarness(t, &stravaAPI{}, map[string]int{"strava:short": 3})
	err := makeFetchSourceHandler(workerConfig{}, h3.client)(context.Background(), h3.w, fetchJob(in))
	if !errors.As(err, &nak) || nak.Reason != "rate_limited_segments" {
		t.Errorf("segment reserve: err = %v", err)
	}
	if n := len(store.results(t, h3, resultsFetch)); n != 1 {
		t.Errorf("results = %d; want activity only", n)
	}

	h4, _ := limitedHarness(t, &stravaAPI{}, map[string]int{"strava:daily": 1})
	_ = makeFetchSourceHandler(workerConfig{}, h4.client)(context.Background(), h4.w, fetchJob(fetchSourceJob{JobID: "a", ExtID: "42"}))
	if err := makeFetchSourceHandler(workerConfig{}, h4.client)(context.Background(), h4.w, fetchJob(fetchSourceJob{JobID: "b", ExtID: "42"})); !errors.As(err, &nak) {
		t.Errorf("daily budget: err = %v", err)
	}
}

func parseJob(t *testing.T, in parseBlobJob) workersdk.Job {
	t.Helper()
	body, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	return workersdk.Job{Subject: subjParseBlob, Body: body}
}

func TestParseBlob(t *testing.T) {
	h, store := fetchHarness(t, &stravaAPI{})
	archive, _ := json.Marshal(archivedActivity{Version: 1, Activity: &stravaActivity{ID: 77, Name: "Archived", SportType: "Run",
		GearID: "g9", Gear: &stravaGearSummary{ID: "g9", Nickname: "Pegasus"}}})
	store.put("raw1", archive)
	store.put("empty", []byte(`{"version":1}`))
	handler := makeParseBlobHandler(workerConfig{WorkerName: workerName})
	ctx := context.Background()

	var in parseBlobJob
	in.JobID, in.UserID = "p1", "u1"
	in.Blob.URL = store.srv.URL + "/raw1"
	if err := handler(ctx, h.w, parseJob(t, in)); err != nil {
		t.Fatalf("fresh url: %v", err)
	}
	res := store.results(t, h, "cairn.results.parse_blob.strava")
	if len(res) != 1 || len(res[0].GetEvents()) != 2 || res[0].GetEvents()[1].GetActivity().GetRef().GetExternalId() != "77" {
		t.Fatalf("results = %v", res)
	}
	// A reparse backfills gear from the archived summary: shoes, named by nickname.
	if g := res[0].GetEvents()[0].GetGear(); g.GetRef().GetExternalId() != "g9" || g.GetPayload().GetKind() != cairnv1.GearKind_GEAR_KIND_SHOE || g.GetPayload().GetName() != "Pegasus" {
		t.Errorf("reparse gear = %v", g)
	}
	if p := res[0].GetEvents()[1].GetActivity().GetPayload(); p.GetGearExternalId() != "g9" || p.GetGearName() != "Pegasus" {
		t.Errorf("reparse payload gear = %q/%q", p.GetGearExternalId(), p.GetGearName())
	}

	// Expired URL: re-presign from the fallback handle.
	in.Blob.ExpiresAt = time.Now().Add(-time.Minute)
	in.Blob.FallbackHandle = "raw1"
	in.ExtID = "override"
	if err := handler(ctx, h.w, parseJob(t, in)); err != nil {
		t.Fatalf("re-presign: %v", err)
	}

	var term *port.TerminalError
	in.Blob.FallbackHandle = ""
	if err := handler(ctx, h.w, parseJob(t, in)); !errors.As(err, &term) || term.Reason != "no_blob_handle" {
		t.Errorf("no handle = %v", err)
	}
	if err := handler(ctx, h.w, workersdk.Job{Body: []byte("{")}); !errors.As(err, &term) {
		t.Errorf("bad payload = %v", err)
	}

	for _, id := range []string{"missing", "empty"} {
		in.Blob.URL, in.Blob.ExpiresAt = store.srv.URL+"/"+id, time.Time{}
		if err := handler(ctx, h.w, parseJob(t, in)); err == nil {
			t.Errorf("%s blob: want error", id)
		}
	}
	store.put("garbage", []byte("not json"))
	in.Blob.URL = store.srv.URL + "/garbage"
	if err := handler(ctx, h.w, parseJob(t, in)); err == nil {
		t.Error("garbage blob: want error")
	}
	in.Blob.URL = "://bad"
	if err := handler(ctx, h.w, parseJob(t, in)); err == nil {
		t.Error("bad url: want error")
	}

	// Presign failure on an expired URL is retryable.
	bare := inmem.New()
	w, _ := workersdk.New(workersdk.Config{Name: workerName, Provider: workerProvider, Bus: bare})
	in.Blob.URL, in.Blob.ExpiresAt, in.Blob.FallbackHandle = "", time.Time{}, "raw1"
	if err := handler(ctx, w, parseJob(t, in)); err == nil || errors.As(err, &term) {
		t.Errorf("presign failure = %v; want retryable", err)
	}
}
