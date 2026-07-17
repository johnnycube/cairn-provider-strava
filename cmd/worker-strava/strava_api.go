package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Strava REST client
//
// Minimum surface for the worker today:
//
//   * GET /api/v3/activities/{id}                          — one activity
//   * GET /api/v3/activities/{id}/streams?keys=...         — stream data
//   * GET /api/v3/athlete/activities?page=N&per_page=K     — list (backfill)
//
// Auth is Bearer token, refresh happens at the workersdk level via
// AuthHandler — by the time this client is called, the access token is
// valid (or the call returns 401, the worker maps it to TerminalError
// needs_reauth, and the next attempt with a fresh token retries).
//
// Errors map onto four typed values so handleFetchSource can switch:
//
//   ErrStravaUnauthorized → workersdk *TerminalError needs_reauth
//   ErrStravaRateLimited  → workersdk *NakWithDelayError (Retry-After)
//   ErrStravaNotFound     → caller publishes deleted_upstream event + ACK
//   ErrStravaServer       → plain error → NAK + retry
//
// 4xx outside of 401/404/429 surface as generic stravaError with the
// response body for diagnostics.
// ---------------------------------------------------------------------------

const defaultStravaBaseURL = "https://www.strava.com/api/v3"

type stravaClient struct {
	baseURL    string
	httpClient *http.Client
	userAgent  string

	// onUsage, when set, receives the read-budget usage Strava reports on
	// every response (X-ReadRateLimit-*, falling back to X-RateLimit-*):
	// (usedShort, limitShort, usedDaily, limitDaily). Wired to the worker's
	// rate limiter so local accounting tracks the provider's real budget
	// instead of a configured guess.
	onUsage func(usedShort, limitShort, usedDaily, limitDaily int)
}

func newStravaClient() *stravaClient {
	return &stravaClient{
		baseURL: defaultStravaBaseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		userAgent: "cairn-strava-worker/dev",
	}
}

// ---------------------------------------------------------------------------
// Typed errors. Each carries enough context for the handler to route.
// ---------------------------------------------------------------------------

var (
	// ErrStravaUnauthorized is returned on a 401 from any endpoint.
	// The worker treats this as "current token is invalid"; the next
	// attempt with a freshly-refreshed token succeeds. If refresh
	// itself fails, the AuthHandler emits the needs_reauth signal.
	ErrStravaUnauthorized = errors.New("strava: 401 unauthorized")

	// ErrStravaNotFound is returned on a 404. The handler publishes
	// a deleted_upstream event and ACKs the job.
	ErrStravaNotFound = errors.New("strava: 404 not found")
)

// StravaRateLimitError is returned on a 429 response. RetryAfter is
// the suggested back-off duration parsed from the Retry-After header
// (falls back to the X-RateLimit-Usage/Limit pair if Retry-After is
// missing).
type StravaRateLimitError struct {
	RetryAfter time.Duration
	// Headers preserved so the handler can call ObserveRateLimit429
	// to record the bucket's authoritative state in NATS-KV.
	Headers map[string]string
}

func (e *StravaRateLimitError) Error() string {
	return fmt.Sprintf("strava: 429 rate limited (retry after %s)", e.RetryAfter)
}

// StravaServerError covers 5xx and unexpected response shapes.
type StravaServerError struct {
	Status int
	Body   string
}

func (e *StravaServerError) Error() string {
	return fmt.Sprintf("strava: %d %s", e.Status, strings.TrimSpace(e.Body))
}

// stravaUnexpectedError carries 4xx responses that don't fit the typed
// buckets above (e.g. 400 invalid_param). Treated as terminal — retry
// won't help if the request itself is malformed.
type stravaUnexpectedError struct {
	Status int
	Body   string
}

func (e *stravaUnexpectedError) Error() string {
	return fmt.Sprintf("strava: %d %s", e.Status, strings.TrimSpace(e.Body))
}

// ---------------------------------------------------------------------------
// Domain types — only the fields the mapper actually reads.
// ---------------------------------------------------------------------------

// stravaActivity mirrors the subset of Strava's DetailedActivity we map
// into domain.ActivitySourcePayload. Unknown fields are silently dropped
// by encoding/json — that's a feature: Strava adds fields without
// notice and we don't want to refuse on those.
type stravaActivity struct {
	ID                   int64   `json:"id"`
	Name                 string  `json:"name"`
	Description          string  `json:"description"`
	Type                 string  `json:"type"`       // legacy enum
	SportType            string  `json:"sport_type"` // newer; preferred when present
	StartDate            string  `json:"start_date"`
	StartDateLocal       string  `json:"start_date_local"`
	Timezone             string  `json:"timezone"`
	ElapsedTime          int     `json:"elapsed_time"`
	MovingTime           int     `json:"moving_time"`
	Distance             float64 `json:"distance"`
	TotalElevationGain   float64 `json:"total_elevation_gain"`
	ElevHigh             float64 `json:"elev_high"`
	ElevLow              float64 `json:"elev_low"`
	AverageSpeed         float64 `json:"average_speed"`
	MaxSpeed             float64 `json:"max_speed"`
	AverageHeartrate     float64 `json:"average_heartrate"`
	MaxHeartrate         float64 `json:"max_heartrate"`
	HasHeartrate         bool    `json:"has_heartrate"`
	AverageWatts         float64 `json:"average_watts"`
	WeightedAverageWatts float64 `json:"weighted_average_watts"`
	MaxWatts             int     `json:"max_watts"`
	AverageCadence       float64 `json:"average_cadence"`
	AverageTemp          float64 `json:"average_temp"`
	Calories             float64 `json:"calories"`
	Kilojoules           float64 `json:"kilojoules"`
	Trainer              bool    `json:"trainer"`
	Commute              bool    `json:"commute"`
	Manual               bool    `json:"manual"`
	WorkoutType          *int    `json:"workout_type"`
	GearID               string  `json:"gear_id"`
	DeviceName           string  `json:"device_name"`
	HasKudoed            bool    `json:"has_kudoed"`
	Athlete              struct {
		ID int64 `json:"id"`
	} `json:"athlete"`
	SegmentEfforts []stravaSegmentEffort `json:"segment_efforts"`
	Laps           []stravaLap           `json:"laps"`
	Photos         struct {
		Primary *struct {
			UniqueID string            `json:"unique_id"`
			Urls     map[string]string `json:"urls"` // size (px, as string) → URL
		} `json:"primary"`
		Count int `json:"count"`
	} `json:"photos"`
}

// stravaLap mirrors a DetailedActivity.laps[] entry — the provider's split
// segmentation (manual or auto laps).
type stravaLap struct {
	LapIndex           int     `json:"lap_index"`
	Name               string  `json:"name"`
	StartDate          string  `json:"start_date"`
	ElapsedTime        int     `json:"elapsed_time"`
	MovingTime         int     `json:"moving_time"`
	Distance           float64 `json:"distance"`
	AverageSpeed       float64 `json:"average_speed"`
	AverageHeartrate   float64 `json:"average_heartrate"`
	MaxHeartrate       float64 `json:"max_heartrate"`
	AverageWatts       float64 `json:"average_watts"`
	AverageCadence     float64 `json:"average_cadence"`
	TotalElevationGain float64 `json:"total_elevation_gain"`
}

// stravaSegmentEffort mirrors a DetailedActivity.segment_efforts[] entry: the
// user's traversal of a segment during this activity, plus the segment summary.
type stravaSegmentEffort struct {
	ID               int64                `json:"id"`
	ElapsedTime      int                  `json:"elapsed_time"`
	MovingTime       int                  `json:"moving_time"`
	StartDate        string               `json:"start_date"`
	Distance         float64              `json:"distance"`
	StartIndex       int                  `json:"start_index"`
	EndIndex         int                  `json:"end_index"`
	AverageHeartrate float64              `json:"average_heartrate"`
	MaxHeartrate     float64              `json:"max_heartrate"`
	AverageWatts     float64              `json:"average_watts"`
	AverageCadence   float64              `json:"average_cadence"`
	Segment          stravaSegmentSummary `json:"segment"`
}

// stravaSegmentSummary is the segment shape embedded in a segment_effort. It
// lacks the polyline — that needs a separate GetSegment call (DetailedSegment).
type stravaSegmentSummary struct {
	ID            int64   `json:"id"`
	Name          string  `json:"name"`
	ActivityType  string  `json:"activity_type"`
	Distance      float64 `json:"distance"`
	AverageGrade  float64 `json:"average_grade"`
	MaximumGrade  float64 `json:"maximum_grade"`
	ElevationHigh float64 `json:"elevation_high"`
	ElevationLow  float64 `json:"elevation_low"`
	ClimbCategory int     `json:"climb_category"`
	Starred       bool    `json:"starred"`
}

// stravaDetailedSegment is GET /segments/{id} — adds the encoded polyline and
// total elevation gain over the summary.
type stravaDetailedSegment struct {
	stravaSegmentSummary
	TotalElevationGain float64 `json:"total_elevation_gain"`
	Map                struct {
		Polyline string `json:"polyline"`
	} `json:"map"`
}

// stravaStream is one channel from the /streams endpoint. Strava's
// key_by_type=true layout returns a JSON object keyed by channel name,
// each value an object with `data` carrying the array.
type stravaStream struct {
	Type         string          `json:"type"`
	Data         json.RawMessage `json:"data"`
	SeriesType   string          `json:"series_type"`
	OriginalSize int             `json:"original_size"`
	Resolution   string          `json:"resolution"`
}

// stravaStreamsResponse is the shape of `key_by_type=true`.
type stravaStreamsResponse map[string]stravaStream

// ---------------------------------------------------------------------------
// Public methods
// ---------------------------------------------------------------------------

// GetActivity fetches a single activity. include_all_efforts=true is required
// for Strava to return the activity's segment_efforts.
func (c *stravaClient) GetActivity(ctx context.Context, accessToken, activityID string) (*stravaActivity, error) {
	resp, err := c.do(ctx, accessToken, http.MethodGet, "/activities/"+activityID+"?include_all_efforts=true", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var act stravaActivity
	if err := json.NewDecoder(resp.Body).Decode(&act); err != nil {
		return nil, fmt.Errorf("decode activity: %w", err)
	}
	return &act, nil
}

// GetSegment fetches one segment's detail (including the encoded polyline) via
// GET /segments/{id}. Rate-limited like every other call — callers Reserve
// before invoking and cache the result (segments are immutable enough that one
// fetch per segment per worker lifetime is plenty).
func (c *stravaClient) GetSegment(ctx context.Context, accessToken, segmentID string) (*stravaDetailedSegment, error) {
	resp, err := c.do(ctx, accessToken, http.MethodGet, "/segments/"+segmentID, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var seg stravaDetailedSegment
	if err := json.NewDecoder(resp.Body).Decode(&seg); err != nil {
		return nil, fmt.Errorf("decode segment: %w", err)
	}
	return &seg, nil
}

// GetActivityStreams fetches the stream channels we care about.
// `key_by_type=true` gives us an object-keyed layout that's friendlier
// to pivot than the default array-of-streams shape.
func (c *stravaClient) GetActivityStreams(ctx context.Context, accessToken, activityID string) (stravaStreamsResponse, error) {
	keys := strings.Join([]string{
		"time", "latlng", "altitude", "distance",
		"velocity_smooth", "heartrate", "cadence", "watts",
		"temp", "grade_smooth",
	}, ",")
	q := url.Values{
		"keys":        []string{keys},
		"key_by_type": []string{"true"},
	}
	path := "/activities/" + activityID + "/streams?" + q.Encode()

	resp, err := c.do(ctx, accessToken, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out stravaStreamsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode streams: %w", err)
	}
	return out, nil
}

// stravaPhoto mirrors one entry from GET /activities/{id}/photos. `urls` and
// `sizes` are keyed by the requested pixel size (as a string); `sizes` carries
// [width, height] per key.
type stravaPhoto struct {
	UniqueID string            `json:"unique_id"`
	Caption  string            `json:"caption"`
	Urls     map[string]string `json:"urls"`
	Sizes    map[string][]int  `json:"sizes"`
}

// GetActivityPhotos returns every photo attached to an activity at up to the
// requested max size. `photo_sources=true` is required for the urls to be
// populated. Costs one API call (caller Reserves first).
func (c *stravaClient) GetActivityPhotos(ctx context.Context, accessToken, activityID string, size int) ([]stravaPhoto, error) {
	q := url.Values{
		"size":          []string{strconv.Itoa(size)},
		"photo_sources": []string{"true"},
	}
	path := "/activities/" + activityID + "/photos?" + q.Encode()

	resp, err := c.do(ctx, accessToken, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out []stravaPhoto
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode photos: %w", err)
	}
	return out, nil
}

// stravaActivitySummary is the listing-endpoint shape (SummaryActivity).
// We extract just the fields the backfill loop fans out to per-activity
// fetch_source jobs.
type stravaActivitySummary struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	StartDate string `json:"start_date"`
}

// ListAthleteActivities returns one page of the athlete's activities.
// `after` is a Unix timestamp; 0 means "all".
func (c *stravaClient) ListAthleteActivities(ctx context.Context, accessToken string, after int64, page, perPage int) ([]stravaActivitySummary, error) {
	q := url.Values{
		"page":     []string{strconv.Itoa(page)},
		"per_page": []string{strconv.Itoa(perPage)},
	}
	if after > 0 {
		q.Set("after", strconv.FormatInt(after, 10))
	}
	path := "/athlete/activities?" + q.Encode()

	resp, err := c.do(ctx, accessToken, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out []stravaActivitySummary
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode listing: %w", err)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

func (c *stravaClient) do(ctx context.Context, accessToken, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")
	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http: %w", err)
	}
	c.reportUsage(resp.Header)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}

	// Drain the body so we can classify and surface it; we close
	// before returning errors.
	bodyBytes, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusUnauthorized:
		return nil, ErrStravaUnauthorized
	case http.StatusNotFound:
		return nil, ErrStravaNotFound
	case http.StatusTooManyRequests:
		return nil, classifyRateLimit(resp.Header)
	}
	if resp.StatusCode >= 500 {
		return nil, &StravaServerError{Status: resp.StatusCode, Body: string(bodyBytes)}
	}
	return nil, &stravaUnexpectedError{Status: resp.StatusCode, Body: string(bodyBytes)}
}

// reportUsage extracts Strava's usage headers and forwards them to onUsage.
// Every worker call is a read, so the read budget (X-ReadRateLimit-*)
// governs; fall back to the overall budget when the read headers are absent.
// Header format: "short,daily" (e.g. Usage "87,613" against Limit "100,1000").
func (c *stravaClient) reportUsage(h http.Header) {
	if c.onUsage == nil {
		return
	}
	limits, ok := parseUsagePair(h.Get("X-ReadRateLimit-Limit"))
	usage, ok2 := parseUsagePair(h.Get("X-ReadRateLimit-Usage"))
	if !ok || !ok2 {
		limits, ok = parseUsagePair(h.Get("X-RateLimit-Limit"))
		usage, ok2 = parseUsagePair(h.Get("X-RateLimit-Usage"))
	}
	if !ok || !ok2 {
		return
	}
	c.onUsage(usage[0], limits[0], usage[1], limits[1])
}

// parseUsagePair parses Strava's "short,daily" header value.
func parseUsagePair(v string) ([2]int, bool) {
	var out [2]int
	parts := strings.SplitN(v, ",", 2)
	if len(parts) != 2 {
		return out, false
	}
	for i, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return out, false
		}
		out[i] = n
	}
	return out, true
}

// nextQuarterHour is when Strava's short window resets: the next quarter-hour
// boundary (:00/:15/:30/:45), plus a small grace so we don't fire exactly on
// the boundary and race the provider's clock.
func nextQuarterHour(now time.Time) time.Time {
	now = now.UTC()
	next := now.Truncate(15 * time.Minute).Add(15 * time.Minute)
	return next.Add(5 * time.Second)
}

// nextMidnightUTC is when Strava's daily window resets.
func nextMidnightUTC(now time.Time) time.Time {
	now = now.UTC()
	next := now.Truncate(24 * time.Hour).Add(24 * time.Hour)
	return next.Add(5 * time.Second)
}

// rateLimitReset decides which budget a 429 tripped from its usage headers and
// returns the matching window reset + bucket name. Daily exhaustion waits for
// midnight UTC; everything else waits for the next quarter-hour.
func rateLimitReset(headers map[string]string, now time.Time) (time.Time, string) {
	limits, ok := parseUsagePair(headers["X-ReadRateLimit-Limit"])
	usage, ok2 := parseUsagePair(headers["X-ReadRateLimit-Usage"])
	if !ok || !ok2 {
		limits, ok = parseUsagePair(headers["X-RateLimit-Limit"])
		usage, ok2 = parseUsagePair(headers["X-RateLimit-Usage"])
	}
	if ok && ok2 && limits[1] > 0 && usage[1] >= limits[1] {
		return nextMidnightUTC(now), "strava:daily"
	}
	return nextQuarterHour(now), "strava:short"
}

// classifyRateLimit reads the Retry-After header (RFC 7231 §7.1.3 — either
// HTTP-date or delta-seconds). Strava usually does NOT send Retry-After;
// RetryAfter stays 0 then and the handler anchors the wait to the actual
// window reset (quarter-hour / midnight UTC) via rateLimitReset.
func classifyRateLimit(h http.Header) error {
	headers := map[string]string{}
	for _, k := range []string{
		"Retry-After",
		"X-RateLimit-Limit",
		"X-RateLimit-Usage",
		"X-ReadRateLimit-Limit",
		"X-ReadRateLimit-Usage",
	} {
		if v := h.Get(k); v != "" {
			headers[k] = v
		}
	}

	var retry time.Duration
	if v := h.Get("Retry-After"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			retry = time.Duration(secs) * time.Second
		} else if t, err := http.ParseTime(v); err == nil && time.Until(t) > 0 {
			retry = time.Until(t)
		}
	}
	return &StravaRateLimitError{RetryAfter: retry, Headers: headers}
}
