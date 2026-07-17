package main

import (
	"context"
	"errors"
	cairnv1 "github.com/johnnycube/cairn-provider-strava/proto/cairn/v1"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestClient(t *testing.T, h http.HandlerFunc) (*stravaClient, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c := newStravaClient()
	c.baseURL = srv.URL
	c.httpClient.Timeout = 2 * time.Second
	return c, srv
}

func TestGetActivity_Success(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/activities/123" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("missing bearer token: %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id": 123,
			"name": "Test Ride",
			"sport_type": "GravelRide",
			"start_date": "2026-05-18T07:00:00Z",
			"elapsed_time": 3700,
			"moving_time": 3600,
			"distance": 42000.5,
			"total_elevation_gain": 500,
			"average_speed": 9.8,
			"has_heartrate": true,
			"average_heartrate": 145,
			"max_heartrate": 178,
			"timezone": "(GMT+01:00) Europe/Berlin"
		}`))
	})

	a, err := c.GetActivity(context.Background(), "test-token", "123")
	if err != nil {
		t.Fatalf("GetActivity: %v", err)
	}
	if a.ID != 123 {
		t.Errorf("id = %d, want 123", a.ID)
	}
	if a.SportType != "GravelRide" {
		t.Errorf("sport_type = %q, want GravelRide", a.SportType)
	}
}

func TestGetActivity_Unauthorized(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})
	_, err := c.GetActivity(context.Background(), "expired", "1")
	if !errors.Is(err, ErrStravaUnauthorized) {
		t.Fatalf("err = %v, want ErrStravaUnauthorized", err)
	}
}

func TestGetActivity_NotFound(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	_, err := c.GetActivity(context.Background(), "ok", "missing")
	if !errors.Is(err, ErrStravaNotFound) {
		t.Fatalf("err = %v, want ErrStravaNotFound", err)
	}
}

func TestGetActivity_RateLimited(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	})
	_, err := c.GetActivity(context.Background(), "ok", "1")
	var rl *StravaRateLimitError
	if !errors.As(err, &rl) {
		t.Fatalf("err = %v, want *StravaRateLimitError", err)
	}
	if rl.RetryAfter != 120*time.Second {
		t.Errorf("RetryAfter = %v, want 120s", rl.RetryAfter)
	}
}

func TestGetActivity_RateLimited_NoHeader(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	})
	_, err := c.GetActivity(context.Background(), "ok", "1")
	var rl *StravaRateLimitError
	// No Retry-After → RetryAfter stays 0; the handler anchors the wait to
	// the actual window reset via rateLimitReset instead.
	if !errors.As(err, &rl) || rl.RetryAfter != 0 {
		t.Fatalf("RetryAfter fallback wrong: %v / %v", err, rl)
	}
}

func TestRateLimitReset_DailyVsShort(t *testing.T) {
	now := time.Date(2026, 7, 6, 12, 20, 0, 0, time.UTC)

	// Short window tripped: wait for the next quarter-hour.
	reset, bucket := rateLimitReset(map[string]string{
		"X-ReadRateLimit-Limit": "100,1000",
		"X-ReadRateLimit-Usage": "100,613",
	}, now)
	if bucket != "strava:short" {
		t.Errorf("bucket = %s, want strava:short", bucket)
	}
	if want := time.Date(2026, 7, 6, 12, 30, 5, 0, time.UTC); !reset.Equal(want) {
		t.Errorf("reset = %v, want %v", reset, want)
	}

	// Daily budget exhausted: wait for midnight UTC.
	reset, bucket = rateLimitReset(map[string]string{
		"X-ReadRateLimit-Limit": "100,1000",
		"X-ReadRateLimit-Usage": "87,1000",
	}, now)
	if bucket != "strava:daily" {
		t.Errorf("bucket = %s, want strava:daily", bucket)
	}
	if want := time.Date(2026, 7, 7, 0, 0, 5, 0, time.UTC); !reset.Equal(want) {
		t.Errorf("reset = %v, want %v", reset, want)
	}

	// No headers at all: assume the short window.
	_, bucket = rateLimitReset(map[string]string{}, now)
	if bucket != "strava:short" {
		t.Errorf("bucket = %s, want strava:short", bucket)
	}
}

func TestReportUsage_PrefersReadHeaders(t *testing.T) {
	var got [4]int
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-RateLimit-Limit", "200,2000")
		w.Header().Set("X-RateLimit-Usage", "150,900")
		w.Header().Set("X-ReadRateLimit-Limit", "100,1000")
		w.Header().Set("X-ReadRateLimit-Usage", "87,613")
		_, _ = w.Write([]byte(`{"id":1}`))
	})
	c.onUsage = func(us, ls, ud, ld int) { got = [4]int{us, ls, ud, ld} }
	if _, err := c.GetActivity(context.Background(), "ok", "1"); err != nil {
		t.Fatalf("GetActivity: %v", err)
	}
	if got != [4]int{87, 100, 613, 1000} {
		t.Errorf("onUsage got %v, want [87 100 613 1000]", got)
	}
}

func TestGetActivity_ServerError(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("upstream down"))
	})
	_, err := c.GetActivity(context.Background(), "ok", "1")
	var se *StravaServerError
	if !errors.As(err, &se) || se.Status != 502 {
		t.Fatalf("err = %v, want StravaServerError 502", err)
	}
}

func TestGetActivityStreams_KeyByType(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawQuery, "key_by_type=true") {
			t.Errorf("expected key_by_type=true in %q", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"time": {"type":"time","data":[0,1,2]},
			"latlng": {"type":"latlng","data":[[48.1,11.5],[48.11,11.51],[48.12,11.52]]},
			"heartrate": {"type":"heartrate","data":[120,125,130]}
		}`))
	})
	streams, err := c.GetActivityStreams(context.Background(), "ok", "1")
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if _, ok := streams["time"]; !ok {
		t.Fatalf("missing time channel")
	}
}

func TestMapSportType(t *testing.T) {
	cases := []struct {
		in        string
		typ       cairnv1.ActivityType
		disc      cairnv1.Discipline
		ebike     bool
		isVirtual bool
	}{
		{"Ride", cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_ROAD, false, false},
		{"GravelRide", cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_GRAVEL, false, false},
		{"EBikeRide", cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_ROAD, true, false},
		{"VirtualRide", cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_ROAD, false, true},
		{"TrailRun", cairnv1.ActivityType_ACTIVITY_TYPE_RUN, cairnv1.Discipline_DISCIPLINE_RUN_TRAIL, false, false},
		{"AlpineSki", cairnv1.ActivityType_ACTIVITY_TYPE_SKI, cairnv1.Discipline_DISCIPLINE_SKI_ALPINE, false, false},
		// Extended sport set.
		{"Snowboard", cairnv1.ActivityType_ACTIVITY_TYPE_SNOWBOARD, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false},
		{"Kayaking", cairnv1.ActivityType_ACTIVITY_TYPE_KAYAK, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false},
		{"StandUpPaddling", cairnv1.ActivityType_ACTIVITY_TYPE_SUP, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false},
		{"Surfing", cairnv1.ActivityType_ACTIVITY_TYPE_SURF, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false},
		{"Golf", cairnv1.ActivityType_ACTIVITY_TYPE_GOLF, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false},
		{"InlineSkate", cairnv1.ActivityType_ACTIVITY_TYPE_SKATE, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false},
		{"Wheelchair", cairnv1.ActivityType_ACTIVITY_TYPE_WHEELCHAIR, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false},
		{"Pickleball", cairnv1.ActivityType_ACTIVITY_TYPE_TENNIS, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false},
		{"", cairnv1.ActivityType_ACTIVITY_TYPE_WORKOUT, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			tp, d, eb, vt := mapSportType(c.in, "")
			if tp != c.typ || d != c.disc || eb != c.ebike || vt != c.isVirtual {
				t.Errorf("mapSportType(%q) = (%v,%v,%v,%v); want (%v,%v,%v,%v)",
					c.in, tp, d, eb, vt, c.typ, c.disc, c.ebike, c.isVirtual)
			}
		})
	}
}

func TestNormaliseTimezone(t *testing.T) {
	if got := normaliseTimezone("(GMT+01:00) Europe/Berlin"); got != "Europe/Berlin" {
		t.Errorf("got %q", got)
	}
	if got := normaliseTimezone("UTC"); got != "UTC" {
		t.Errorf("got %q", got)
	}
	if got := normaliseTimezone(""); got != "UTC" {
		t.Errorf("empty fallback wrong: %q", got)
	}
}

func TestMapActivityToPayload(t *testing.T) {
	a := &stravaActivity{
		ID:                 42,
		Name:               "Big Day",
		Description:        "Sehr lang.",
		SportType:          "Run",
		StartDate:          "2026-05-18T07:00:00Z",
		Timezone:           "(GMT+02:00) Europe/Berlin",
		ElapsedTime:        7200,
		MovingTime:         6900,
		Distance:           20000,
		TotalElevationGain: 250,
		AverageSpeed:       2.9,
		HasHeartrate:       true,
		AverageHeartrate:   148,
		MaxHeartrate:       182,
	}
	p := mapActivityToPayload(a)

	if p.Type != cairnv1.ActivityType_ACTIVITY_TYPE_RUN {
		t.Errorf("Type = %v, want RUN", p.Type)
	}
	if p.Discipline != cairnv1.Discipline_DISCIPLINE_RUN_ROAD {
		t.Errorf("Discipline = %v, want RUN_ROAD", p.Discipline)
	}
	if p.Timezone != "Europe/Berlin" {
		t.Errorf("Timezone = %q", p.Timezone)
	}
	if p.ElapsedDuration.AsDuration() != 7200*time.Second {
		t.Errorf("ElapsedDuration = %v", p.ElapsedDuration.AsDuration())
	}
	if p.Summary.AvgHeartRateBpm == nil || *p.Summary.AvgHeartRateBpm != 148 {
		t.Errorf("AvgHeartRateBpm wrong: %v", p.Summary.AvgHeartRateBpm)
	}
	if p.EndTime.AsTime().Sub(p.StartTime.AsTime()) != 7200*time.Second {
		t.Errorf("End-Start = %v, want 7200s", p.EndTime.AsTime().Sub(p.StartTime.AsTime()))
	}
}

func TestMapStreamsToProto(t *testing.T) {
	streams := stravaStreamsResponse{
		"time":      {Type: "time", Data: []byte(`[0,1,2]`)},
		"latlng":    {Type: "latlng", Data: []byte(`[[48.1,11.5],[48.2,11.6],[48.3,11.7]]`)},
		"heartrate": {Type: "heartrate", Data: []byte(`[100,110,120]`)},
		"distance":  {Type: "distance", Data: []byte(`[0,5.5,11]`)},
	}
	st := mapStreamsToProto(streams)
	if st == nil || st.SampleCount != 3 {
		t.Fatalf("sample_count wrong: %v", st)
	}
	if len(st.TimeS) != 3 || st.TimeS[0] != 0 {
		t.Errorf("time_s wrong: %v", st.TimeS)
	}
	if len(st.HeartRateBpm) != 3 || st.HeartRateBpm[2] != 120 {
		t.Errorf("hr wrong: %v", st.HeartRateBpm)
	}
	if len(st.Latitude) != 3 || st.Latitude[1] != 48.2 {
		t.Errorf("latitude wrong: %v", st.Latitude)
	}
}

func TestMapStreamsToProto_NoTime(t *testing.T) {
	// Without the time channel the function returns nil — every other
	// channel keys off the time index.
	streams := stravaStreamsResponse{
		"heartrate": {Type: "heartrate", Data: []byte(`[100,110]`)},
	}
	if st := mapStreamsToProto(streams); st != nil {
		t.Errorf("want nil, got %v", st)
	}
}
