package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/johnnycube/cairn-provider-strava/internal/inmem"
	"github.com/johnnycube/cairn-provider-strava/internal/port"
	"github.com/johnnycube/cairn-provider-strava/internal/workersdk"
	cairnv1 "github.com/johnnycube/cairn-provider-strava/proto/cairn/v1"
	workerv1 "github.com/johnnycube/cairn-provider-strava/proto/cairn/worker/v1"
)

// harness is a worker on an in-memory bus talking to a fake Strava API.
type harness struct {
	w      *workersdk.Worker
	bus    *inmem.Bus
	client *stravaClient

	mu       sync.Mutex
	messages map[string][]port.Message // by subject
}

func newHarness(t *testing.T, strava http.HandlerFunc) *harness {
	t.Helper()
	ctx := context.Background()
	h := &harness{bus: inmem.New(), messages: map[string][]port.Message{}}

	if _, err := h.bus.RespondTo(ctx, "cairn.tokens.strava.get", func(context.Context, []byte) ([]byte, error) {
		return json.Marshal(map[string]any{"access_token": "tok", "expires_at": time.Now().Add(time.Hour)})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.bus.Subscribe(ctx, port.ConsumerConfig{Subject: "cairn.>"}, func(_ context.Context, m port.Message) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.messages[m.Subject] = append(h.messages[m.Subject], m)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	w, err := workersdk.New(workersdk.Config{Name: workerName, Provider: workerProvider, Bus: h.bus})
	if err != nil {
		t.Fatal(err)
	}
	h.w = w
	h.client = newTestClient(t, strava)
	return h
}

func (h *harness) published(subject string) []port.Message {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.messages[subject]
}

// fetchJobs decodes the fetch_source sub-jobs enqueued so far.
func (h *harness) fetchJobs(t *testing.T) []fetchSourceJob {
	t.Helper()
	var out []fetchSourceJob
	for _, m := range h.published(subjFetchSource) {
		var j fetchSourceJob
		if err := json.Unmarshal(m.Body, &j); err != nil {
			t.Fatal(err)
		}
		out = append(out, j)
	}
	return out
}

// listingServer serves /athlete/activities from pages (1-based) and records
// each request's query.
func listingServer(pages [][]stravaActivitySummary, queries *[]map[string]string) http.HandlerFunc {
	var mu sync.Mutex
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/athlete/activities" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query()
		mu.Lock()
		*queries = append(*queries, map[string]string{"page": q.Get("page"), "per_page": q.Get("per_page"), "after": q.Get("after")})
		mu.Unlock()
		page, _ := strconv.Atoi(q.Get("page"))
		var out []stravaActivitySummary
		if page >= 1 && page <= len(pages) {
			out = pages[page-1]
		}
		_ = json.NewEncoder(w).Encode(out)
	}
}

func summaries(from, n int) []stravaActivitySummary {
	out := make([]stravaActivitySummary, n)
	for i := range out {
		id := int64(from + i)
		out[i] = stravaActivitySummary{ID: id, StartDate: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(id) * time.Hour).Format(time.RFC3339)}
	}
	return out
}

func TestBackfill_StopsOnShortPageAndPublishesResult(t *testing.T) {
	var queries []map[string]string
	h := newHarness(t, listingServer([][]stravaActivitySummary{summaries(1, 2), summaries(3, 1)}, &queries))

	body, _ := json.Marshal(backfillJob{JobID: "b1", AccountID: "acct", UserID: "u1", PageSize: 2})
	job := workersdk.Job{Subject: subjBackfill, Body: body}
	if err := makeBackfillHandler(workerConfig{WorkerName: workerName}, h.client)(context.Background(), h.w, job); err != nil {
		t.Fatalf("backfill: %v", err)
	}

	jobs := h.fetchJobs(t)
	if len(jobs) != 3 {
		t.Fatalf("enqueued %d fetch jobs; want 3", len(jobs))
	}
	if j := jobs[0]; j.JobID != "b1:1" || j.ExtID != "1" || j.Reason != "backfill" || !j.FetchStreams || j.AccountID != "acct" {
		t.Errorf("first fetch job = %+v", j)
	}
	if len(queries) != 2 {
		t.Errorf("listed %d pages; want 2 (short page ends the walk)", len(queries))
	}
	if got := h.published("cairn.results.backfill.strava"); len(got) != 1 {
		t.Fatalf("published %d backfill results; want 1", len(got))
	}
}

func TestBackfill_CapsAtMaxActivities(t *testing.T) {
	var queries []map[string]string
	h := newHarness(t, listingServer([][]stravaActivitySummary{summaries(1, 3), summaries(4, 3)}, &queries))

	body, _ := json.Marshal(backfillJob{JobID: "b1", AccountID: "acct", PageSize: 3, MaxActivities: 2})
	if err := makeBackfillHandler(workerConfig{}, h.client)(context.Background(), h.w, workersdk.Job{Subject: subjBackfill, Body: body}); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if n := len(h.fetchJobs(t)); n != 2 {
		t.Errorf("enqueued %d; want 2", n)
	}
	if len(queries) != 1 {
		t.Errorf("listed %d pages; want 1", len(queries))
	}
}

func TestBackfill_ListingNotFoundAcksWithoutResult(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) })

	body, _ := json.Marshal(backfillJob{JobID: "b1", AccountID: "acct"})
	if err := makeBackfillHandler(workerConfig{}, h.client)(context.Background(), h.w, workersdk.Job{Subject: subjBackfill, Body: body}); err != nil {
		t.Fatalf("backfill: %v; want nil (ack)", err)
	}
	if got := h.published("cairn.results.backfill.strava"); len(got) != 0 {
		t.Errorf("published %d results; want none", len(got))
	}
}

func TestBackfill_ListingUnauthorizedIsTerminal(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) })

	body, _ := json.Marshal(backfillJob{JobID: "b1", AccountID: "acct"})
	err := makeBackfillHandler(workerConfig{}, h.client)(context.Background(), h.w, workersdk.Job{Subject: subjBackfill, Body: body})
	var term *port.TerminalError
	if !errors.As(err, &term) || term.Reason != "needs_reauth" {
		t.Fatalf("err = %v; want needs_reauth terminal", err)
	}
}

func TestBackfill_BadPayloadIsTerminal(t *testing.T) {
	h := newHarness(t, func(http.ResponseWriter, *http.Request) {})
	err := makeBackfillHandler(workerConfig{}, h.client)(context.Background(), h.w, workersdk.Job{Body: []byte("{")})
	var term *port.TerminalError
	if !errors.As(err, &term) || term.Reason != "bad_payload" {
		t.Fatalf("err = %v; want bad_payload", err)
	}
}

func TestReconcile_SkipsKnownAndAdvancesWatermark(t *testing.T) {
	var queries []map[string]string
	h := newHarness(t, listingServer([][]stravaActivitySummary{summaries(1, 3)}, &queries))

	wm := time.Now().Add(-2 * time.Hour)
	body, _ := json.Marshal(reconcileJob{JobID: "r1", AccountID: "acct", UserID: "u1", Watermark: &wm, KnownExtIDs: []string{"2"}})
	if err := makeReconcileHandler(workerConfig{}, h.client)(context.Background(), h.w, workersdk.Job{Subject: subjReconcile, Body: body}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	jobs := h.fetchJobs(t)
	if len(jobs) != 2 || jobs[0].ExtID != "1" || jobs[1].ExtID != "3" || jobs[1].Reason != "reconcile" {
		t.Fatalf("fetch jobs = %+v; want ext ids 1 and 3", jobs)
	}
	if want := strconv.FormatInt(wm.Add(-time.Hour).Unix(), 10); queries[0]["after"] != want || queries[0]["per_page"] != "100" {
		t.Errorf("listing query = %v; want after=%s per_page=100", queries[0], want)
	}

	results := h.published("cairn.results.reconcile.strava.acct")
	if len(results) != 1 {
		t.Fatalf("published %d reconcile results; want 1", len(results))
	}
	var res workerv1.JobResult
	if err := protojson.Unmarshal(results[0].Body, &res); err != nil {
		t.Fatal(err)
	}
	// Watermark from the job is older than the listed activities in 2026-01;
	// the newest enqueued one (id 3) wins only if it is later.
	wantWM := wm
	if t3 := parseStravaTime(summaries(3, 1)[0].StartDate); t3.After(wm) {
		wantWM = t3
	}
	if got := res.GetNewWatermark().AsTime(); !got.Equal(wantWM.Truncate(time.Nanosecond)) {
		t.Errorf("watermark = %v; want %v", got, wantWM)
	}
}

func TestReconcile_StopsAtMaxEnqueue(t *testing.T) {
	var queries []map[string]string
	full := summaries(1, 100)
	h := newHarness(t, listingServer([][]stravaActivitySummary{full, summaries(101, 100)}, &queries))

	body, _ := json.Marshal(reconcileJob{JobID: "r1", AccountID: "acct", MaxEnqueue: 50})
	if err := makeReconcileHandler(workerConfig{}, h.client)(context.Background(), h.w, workersdk.Job{Subject: subjReconcile, Body: body}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// The cap is checked per page, so the whole first page is enqueued.
	if n := len(h.fetchJobs(t)); n != 100 {
		t.Errorf("enqueued %d; want 100", n)
	}
	if len(queries) != 1 {
		t.Errorf("listed %d pages; want 1", len(queries))
	}
}

func TestReconcileWindowStart(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	floor := now.Add(-reconcileLookbackFloor).Unix()
	recent := now.Add(-3 * time.Hour)
	ancient := now.Add(-365 * 24 * time.Hour)

	if got := reconcileWindowStart(nil, now); got != floor {
		t.Errorf("no watermark: %d; want floor %d", got, floor)
	}
	if got := reconcileWindowStart(&recent, now); got != recent.Add(-time.Hour).Unix() {
		t.Errorf("recent watermark: %d; want watermark-1h", got)
	}
	if got := reconcileWindowStart(&ancient, now); got != floor {
		t.Errorf("ancient watermark: %d; want floor %d", got, floor)
	}
	if got := reconcileWindowStart(nil, time.Unix(0, 0)); got != 0 {
		t.Errorf("pre-epoch window: %d; want 0", got)
	}
}

func TestDiscover_WalksUntilEmptyPage(t *testing.T) {
	var queries []map[string]string
	h := newHarness(t, listingServer([][]stravaActivitySummary{summaries(1, 2), summaries(3, 1)}, &queries))

	out, err := makeDiscoverHandler(h.w, h.client)(context.Background(), []byte(`{"account_id":"acct"}`))
	if err != nil {
		t.Fatal(err)
	}
	var resp discoverResponse
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.Complete || len(resp.Items) != 3 || resp.Items[2].ExternalID != "3" || resp.Error != "" {
		t.Fatalf("resp = %+v; want 3 items, complete", resp)
	}
	if len(queries) != 3 || queries[0]["page"] != "1" || queries[0]["per_page"] != "200" {
		t.Errorf("queries = %v; want pages 1..3 at 200", queries)
	}
}

func TestDiscover_BatchBoundary(t *testing.T) {
	var queries []map[string]string
	pages := make([][]stravaActivitySummary, discoverMaxPagesPerCall+5)
	for i := range pages {
		pages[i] = summaries(i+1, 1)
	}
	h := newHarness(t, listingServer(pages, &queries))

	out, err := makeDiscoverHandler(h.w, h.client)(context.Background(), []byte(`{"account_id":"acct","start_page":3}`))
	if err != nil {
		t.Fatal(err)
	}
	var resp discoverResponse
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Complete || len(resp.Items) != discoverMaxPagesPerCall || resp.NextPage != 3+discoverMaxPagesPerCall {
		t.Fatalf("resp complete=%v items=%d next=%d", resp.Complete, len(resp.Items), resp.NextPage)
	}
}

func TestDiscover_ListingErrorOnFirstPage(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) })
	out, err := makeDiscoverHandler(h.w, h.client)(context.Background(), []byte(`{"account_id":"acct"}`))
	if err != nil {
		t.Fatal(err)
	}
	var resp discoverResponse
	_ = json.Unmarshal(out, &resp)
	if resp.Error == "" || len(resp.Items) != 0 {
		t.Fatalf("resp = %+v; want an error", resp)
	}
}

func TestDiscoverReserveFailed(t *testing.T) {
	nak := &port.NakWithDelayError{Reason: "rate_limited", Delay: time.Second}
	other := fmt.Errorf("limiter down")
	some := []discoverItem{{ExternalID: "1"}}

	cases := []struct {
		name      string
		items     []discoverItem
		page      int
		err       error
		wantError bool
		wantRL    bool
	}{
		{"rate limited first page", nil, 1, nak, true, false},
		{"rate limited later empty page", nil, 4, nak, false, true},
		{"rate limited with items", some, 2, nak, false, true},
		{"other error, nothing yet", nil, 3, other, true, false},
		{"other error with items", some, 3, other, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := discoverReserveFailed(c.items, c.page, c.err)
			if (got.Error != "") != c.wantError || got.RateLimited != c.wantRL {
				t.Fatalf("got %+v", got)
			}
			if c.wantRL && got.NextPage != c.page {
				t.Errorf("next page = %d; want %d", got.NextPage, c.page)
			}
		})
	}
}

func TestMapSportType_Lookup(t *testing.T) {
	cases := []struct {
		sport, legacy string
		typ           cairnv1.ActivityType
		disc          cairnv1.Discipline
		ebike, virt   bool
	}{
		{"EMountainBikeRide", "", cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_MTB, true, false},
		{"VirtualRun", "", cairnv1.ActivityType_ACTIVITY_TYPE_RUN, cairnv1.Discipline_DISCIPLINE_RUN_ROAD, false, true},
		{"", "Hike", cairnv1.ActivityType_ACTIVITY_TYPE_HIKE, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false},
		{"TableTennis", "Workout", cairnv1.ActivityType_ACTIVITY_TYPE_TENNIS, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false},
		{"StairStepper", "", cairnv1.ActivityType_ACTIVITY_TYPE_WORKOUT, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false},
	}
	for _, c := range cases {
		typ, disc, ebike, virt := mapSportType(c.sport, c.legacy)
		if typ != c.typ || disc != c.disc || ebike != c.ebike || virt != c.virt {
			t.Errorf("mapSportType(%q, %q) = %v %v %v %v", c.sport, c.legacy, typ, disc, ebike, virt)
		}
	}
}

func TestMapLapsToProto_ZeroMetricsStayUnset(t *testing.T) {
	laps := mapLapsToProto([]stravaLap{
		{LapIndex: 1, Distance: 1000, AverageHeartrate: 150.4, AverageWatts: -1},
		{LapIndex: 2},
	})
	if len(laps) != 2 {
		t.Fatalf("laps = %d", len(laps))
	}
	if laps[0].GetDistanceM() != 1000 || laps[0].GetAvgHeartRateBpm() != 150 || laps[0].AvgPowerW != nil {
		t.Errorf("lap 1 = %v", laps[0])
	}
	if laps[1].DistanceM != nil || laps[1].AvgHeartRateBpm != nil || laps[1].GetIndex() != 2 {
		t.Errorf("lap 2 = %v", laps[1])
	}
	if mapLapsToProto(nil) != nil {
		t.Error("nil laps should map to nil")
	}
}

func TestMapStreamsToProto_ChannelOrder(t *testing.T) {
	raw := func(v string) stravaStream { return stravaStream{Data: json.RawMessage(v)} }
	st := mapStreamsToProto(stravaStreamsResponse{
		"time":         raw(`[0,1]`),
		"latlng":       raw(`[[1,2],[3,4]]`),
		"grade_smooth": raw(`[0.5,0.6]`),
		"heartrate":    raw(`[120,121]`),
		"altitude":     raw(`[10,11]`),
	})
	want := []cairnv1.StreamChannel{
		cairnv1.StreamChannel_STREAM_CHANNEL_LATITUDE,
		cairnv1.StreamChannel_STREAM_CHANNEL_LONGITUDE,
		cairnv1.StreamChannel_STREAM_CHANNEL_ALTITUDE,
		cairnv1.StreamChannel_STREAM_CHANNEL_HEART_RATE,
		cairnv1.StreamChannel_STREAM_CHANNEL_GRADE,
	}
	got := st.GetChannels()
	if len(got) != len(want) {
		t.Fatalf("channels = %v; want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("channels = %v; want %v", got, want)
		}
	}
	if st.GetLongitude()[1] != 4 || st.GetHeartRateBpm()[0] != 120 || st.GetSampleCount() != 2 {
		t.Errorf("stream = %v", st)
	}
	if mapStreamsToProto(nil) != nil {
		t.Error("no time channel should map to nil")
	}
}

func TestClampI32(t *testing.T) {
	if clampI32(math.MaxInt32+1) != math.MaxInt32 || clampI32(math.MinInt32-1) != math.MinInt32 || clampI32(7) != 7 {
		t.Error("clampI32 does not saturate")
	}
}
