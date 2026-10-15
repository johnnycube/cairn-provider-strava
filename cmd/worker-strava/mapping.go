package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	cairnv1 "github.com/johnnycube/cairn-provider-strava/proto/cairn/v1"
	workerv1 "github.com/johnnycube/cairn-provider-strava/proto/cairn/worker/v1"
)

// ---------------------------------------------------------------------------
// Strava → typed cairn.v1 mapping
//
// The worker emits the canonical proto entities directly; there is no
// hand-rolled JSON wire form. The Core decodes the typed JobResult.
// ---------------------------------------------------------------------------

// mapActivityToPayload converts a Strava DetailedActivity into the typed
// cairn.v1.ActivitySourcePayload.
func mapActivityToPayload(a *stravaActivity) *cairnv1.ActivitySourcePayload {
	typ, disc, isEbike, isVirtual := mapSportType(a.SportType, a.Type)

	start := parseStravaTime(a.StartDate)
	end := start.Add(time.Duration(a.ElapsedTime) * time.Second)

	p := &cairnv1.ActivitySourcePayload{
		Type:               typ,
		Discipline:         disc,
		IsVirtual:          isVirtual,
		IsEbike:            isEbike,
		IsCommute:          a.Commute,
		IsRace:             workoutTypeIsRace(a.WorkoutType, typ),
		ProviderNativeType: a.SportType,
		Title:              a.Name,
		Description:        a.Description,
		StartTime:          timestamppb.New(start),
		EndTime:            timestamppb.New(end),
		ElapsedDuration:    durationpb.New(time.Duration(a.ElapsedTime) * time.Second),
		MovingDuration:     durationpb.New(time.Duration(a.MovingTime) * time.Second),
		Timezone:           normaliseTimezone(a.Timezone),
		Summary:            mapSummary(a),
	}

	// CustomSubtype only when we fell back to WORKOUT — preserve the raw
	// sport_type so the frontend can render "(StairStepper)" etc.
	if typ == cairnv1.ActivityType_ACTIVITY_TYPE_WORKOUT && a.SportType != "" {
		p.CustomSubtype = a.SportType
	}
	p.Laps = mapLapsToProto(a.Laps)
	return p
}

// mapLapsToProto maps Strava laps (manual/auto splits) to the typed payload.
func mapLapsToProto(laps []stravaLap) []*cairnv1.ActivityLap {
	if len(laps) == 0 {
		return nil
	}
	out := make([]*cairnv1.ActivityLap, 0, len(laps))
	for _, l := range laps {
		out = append(out, mapLap(l))
	}
	return out
}

// mapLap maps one Strava lap; zero metrics stay unset.
func mapLap(l stravaLap) *cairnv1.ActivityLap {
	lap := &cairnv1.ActivityLap{
		Index:           clampI32(l.LapIndex),
		Label:           l.Name,
		StartTime:       timestamppb.New(parseStravaTime(l.StartDate)),
		ElapsedDuration: durationpb.New(time.Duration(l.ElapsedTime) * time.Second),
		MovingDuration:  durationpb.New(time.Duration(l.MovingTime) * time.Second),
		DistanceM:       optF64(max(l.Distance, 0)),
		AvgSpeedMps:     optF64(max(l.AverageSpeed, 0)),
		AvgHeartRateBpm: optI32FromFloat(max(l.AverageHeartrate, 0)),
		MaxHeartRateBpm: optI32FromFloat(max(l.MaxHeartrate, 0)),
		AvgPowerW:       optI32FromFloat(max(l.AverageWatts, 0)),
		AvgCadence:      optI32FromFloat(max(l.AverageCadence, 0)),
		ElevationGainM:  optF64(max(l.TotalElevationGain, 0)),
	}
	return lap
}

// mapStreamsToProto pivots Strava's column-keyed streams into the typed
// column-oriented cairn.v1.ActivityStream. Requires the `time` channel —
// without it there's no shared index. Returns nil when streams are empty.
func mapStreamsToProto(streams stravaStreamsResponse) *cairnv1.ActivityStream {
	timeCh, ok := streams["time"]
	if !ok || len(timeCh.Data) == 0 {
		return nil
	}
	var seconds []float64
	if err := json.Unmarshal(timeCh.Data, &seconds); err != nil {
		return nil
	}
	n := len(seconds)

	st := &cairnv1.ActivityStream{
		SampleCount:  clampI32(n),
		ResolutionHz: 1.0,
		TimeS:        seconds,
	}
	var channels []cairnv1.StreamChannel

	// latlng is a [N][2] of [lat, lon] pairs in Strava's response.
	if lat, lon := latLngCols(streams, n); lat != nil {
		st.Latitude, st.Longitude = lat, lon
		channels = append(channels,
			cairnv1.StreamChannel_STREAM_CHANNEL_LATITUDE,
			cairnv1.StreamChannel_STREAM_CHANNEL_LONGITUDE)
	}

	// Each setter fills its column and reports whether the channel is present;
	// the slice order is the channel order in the output.
	setters := []struct {
		ch  cairnv1.StreamChannel
		set func() bool
	}{
		{cairnv1.StreamChannel_STREAM_CHANNEL_ALTITUDE, func() bool { st.AltitudeM = floatCol(streams, "altitude", n); return st.AltitudeM != nil }},
		{cairnv1.StreamChannel_STREAM_CHANNEL_DISTANCE, func() bool { st.DistanceM = floatCol(streams, "distance", n); return st.DistanceM != nil }},
		{cairnv1.StreamChannel_STREAM_CHANNEL_SPEED, func() bool { st.SpeedMps = floatCol(streams, "velocity_smooth", n); return st.SpeedMps != nil }},
		{cairnv1.StreamChannel_STREAM_CHANNEL_HEART_RATE, func() bool { st.HeartRateBpm = intCol(streams, "heartrate", n); return st.HeartRateBpm != nil }},
		{cairnv1.StreamChannel_STREAM_CHANNEL_POWER, func() bool { st.PowerW = intCol(streams, "watts", n); return st.PowerW != nil }},
		{cairnv1.StreamChannel_STREAM_CHANNEL_CADENCE, func() bool { st.Cadence = intCol(streams, "cadence", n); return st.Cadence != nil }},
		{cairnv1.StreamChannel_STREAM_CHANNEL_TEMPERATURE, func() bool { st.TemperatureC = floatCol(streams, "temp", n); return st.TemperatureC != nil }},
		{cairnv1.StreamChannel_STREAM_CHANNEL_GRADE, func() bool { st.Grade = floatCol(streams, "grade_smooth", n); return st.Grade != nil }},
	}
	for _, c := range setters {
		if c.set() {
			channels = append(channels, c.ch)
		}
	}

	st.Channels = channels
	return st
}

// ---------------------------------------------------------------------------
// Field-by-field helpers
// ---------------------------------------------------------------------------

// mapSportType returns (type, discipline, isEbike, isVirtual). Strava's
// sport_type is preferred; legacy `type` is the fallback. Unknown
// sport_types fall back to (WORKOUT, UNSPECIFIED) so we never lose the
// activity — the original sport_type lands in custom_subtype.
func mapSportType(sportType, legacyType string) (typ cairnv1.ActivityType, discipline cairnv1.Discipline, isEbike, isVirtual bool) {
	src := sportType
	if src == "" {
		src = legacyType
	}
	if m, ok := sportTypes[src]; ok {
		return m.typ, m.discipline, m.ebike, m.virtual
	}
	return cairnv1.ActivityType_ACTIVITY_TYPE_WORKOUT, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false
}

type sportMapping struct {
	typ        cairnv1.ActivityType
	discipline cairnv1.Discipline
	ebike      bool
	virtual    bool
}

// sportTypes maps Strava sport_type names to Cairn types. The extended sports
// (Snowboard onwards) map to coarse types with no discipline yet.
var sportTypes = map[string]sportMapping{
	"Ride":              {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, discipline: cairnv1.Discipline_DISCIPLINE_RIDE_ROAD},
	"GravelRide":        {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, discipline: cairnv1.Discipline_DISCIPLINE_RIDE_GRAVEL},
	"MountainBikeRide":  {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, discipline: cairnv1.Discipline_DISCIPLINE_RIDE_MTB},
	"EMountainBikeRide": {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, discipline: cairnv1.Discipline_DISCIPLINE_RIDE_MTB, ebike: true},
	"EBikeRide":         {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, discipline: cairnv1.Discipline_DISCIPLINE_RIDE_ROAD, ebike: true},
	"VirtualRide":       {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, discipline: cairnv1.Discipline_DISCIPLINE_RIDE_ROAD, virtual: true},
	"TrackRide":         {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, discipline: cairnv1.Discipline_DISCIPLINE_RIDE_TRACK},
	"Velomobile":        {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, discipline: cairnv1.Discipline_DISCIPLINE_RIDE_TRACK},
	"Run":               {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RUN, discipline: cairnv1.Discipline_DISCIPLINE_RUN_ROAD},
	"TrailRun":          {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RUN, discipline: cairnv1.Discipline_DISCIPLINE_RUN_TRAIL},
	"VirtualRun":        {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RUN, discipline: cairnv1.Discipline_DISCIPLINE_RUN_ROAD, virtual: true},
	"Hike":              {typ: cairnv1.ActivityType_ACTIVITY_TYPE_HIKE, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Walk":              {typ: cairnv1.ActivityType_ACTIVITY_TYPE_WALK, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Swim":              {typ: cairnv1.ActivityType_ACTIVITY_TYPE_SWIM, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"AlpineSki":         {typ: cairnv1.ActivityType_ACTIVITY_TYPE_SKI, discipline: cairnv1.Discipline_DISCIPLINE_SKI_ALPINE},
	"NordicSki":         {typ: cairnv1.ActivityType_ACTIVITY_TYPE_SKI, discipline: cairnv1.Discipline_DISCIPLINE_SKI_NORDIC},
	"RollerSki":         {typ: cairnv1.ActivityType_ACTIVITY_TYPE_SKI, discipline: cairnv1.Discipline_DISCIPLINE_SKI_NORDIC},
	"BackcountrySki":    {typ: cairnv1.ActivityType_ACTIVITY_TYPE_SKI, discipline: cairnv1.Discipline_DISCIPLINE_SKI_BACKCOUNTRY},
	"Rowing":            {typ: cairnv1.ActivityType_ACTIVITY_TYPE_ROW, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"VirtualRow":        {typ: cairnv1.ActivityType_ACTIVITY_TYPE_ROW, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Snowboard":         {typ: cairnv1.ActivityType_ACTIVITY_TYPE_SNOWBOARD, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Kayaking":          {typ: cairnv1.ActivityType_ACTIVITY_TYPE_KAYAK, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Canoeing":          {typ: cairnv1.ActivityType_ACTIVITY_TYPE_KAYAK, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"StandUpPaddling":   {typ: cairnv1.ActivityType_ACTIVITY_TYPE_SUP, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Surfing":           {typ: cairnv1.ActivityType_ACTIVITY_TYPE_SURF, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Windsurf":          {typ: cairnv1.ActivityType_ACTIVITY_TYPE_SURF, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Kitesurf":          {typ: cairnv1.ActivityType_ACTIVITY_TYPE_SURF, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Golf":              {typ: cairnv1.ActivityType_ACTIVITY_TYPE_GOLF, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"RockClimbing":      {typ: cairnv1.ActivityType_ACTIVITY_TYPE_CLIMB, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"InlineSkate":       {typ: cairnv1.ActivityType_ACTIVITY_TYPE_SKATE, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"IceSkate":          {typ: cairnv1.ActivityType_ACTIVITY_TYPE_SKATE, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Skateboard":        {typ: cairnv1.ActivityType_ACTIVITY_TYPE_SKATE, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Elliptical":        {typ: cairnv1.ActivityType_ACTIVITY_TYPE_ELLIPTICAL, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Wheelchair":        {typ: cairnv1.ActivityType_ACTIVITY_TYPE_WHEELCHAIR, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Handcycle":         {typ: cairnv1.ActivityType_ACTIVITY_TYPE_WHEELCHAIR, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Tennis":            {typ: cairnv1.ActivityType_ACTIVITY_TYPE_TENNIS, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Pickleball":        {typ: cairnv1.ActivityType_ACTIVITY_TYPE_TENNIS, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Squash":            {typ: cairnv1.ActivityType_ACTIVITY_TYPE_TENNIS, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Badminton":         {typ: cairnv1.ActivityType_ACTIVITY_TYPE_TENNIS, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"Racquetball":       {typ: cairnv1.ActivityType_ACTIVITY_TYPE_TENNIS, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
	"TableTennis":       {typ: cairnv1.ActivityType_ACTIVITY_TYPE_TENNIS, discipline: cairnv1.Discipline_DISCIPLINE_UNSPECIFIED},
}

// workoutTypeIsRace decodes Strava's `workout_type`. Ride: 11=race. Run: 1=race.
func workoutTypeIsRace(wt *int, typ cairnv1.ActivityType) bool {
	if wt == nil {
		return false
	}
	switch typ {
	case cairnv1.ActivityType_ACTIVITY_TYPE_RIDE:
		return *wt == 11
	case cairnv1.ActivityType_ACTIVITY_TYPE_RUN:
		return *wt == 1
	}
	return false
}

// normaliseTimezone strips Strava's "(GMT+01:00) " prefix, leaving the IANA name.
func normaliseTimezone(raw string) string {
	if raw == "" {
		return "UTC"
	}
	if i := strings.Index(raw, ") "); i > 0 {
		return raw[i+2:]
	}
	return raw
}

// ---------------------------------------------------------------------------
// Segment + segment-effort mapping
// ---------------------------------------------------------------------------

// mapSegmentToProto converts a Strava DetailedSegment (with polyline) into the
// typed SegmentImport. The Core stores it as an external (provider-mirrored)
// segment and derives PostGIS geometry from the encoded polyline.
func mapSegmentToProto(s *stravaDetailedSegment) *workerv1.SegmentImport {
	return &workerv1.SegmentImport{
		Name:            s.Name,
		ActivityType:    segmentActivityType(s.ActivityType),
		EncodedPolyline: s.Map.Polyline,
		DistanceM:       s.Distance,
		ElevationGainM:  optF64(s.TotalElevationGain),
		AvgGrade:        optF64(s.AverageGrade),
		MaxGrade:        optF64(s.MaximumGrade),
		ClimbCategory:   mapClimbCategory(s.ClimbCategory),
		Starred:         s.Starred,
	}
}

// mapSegmentEffortToProto converts one DetailedActivity.segment_efforts[] entry
// into the typed SegmentEffortImport (the user's traversal of the segment).
func mapSegmentEffortToProto(e *stravaSegmentEffort) *workerv1.SegmentEffortImport {
	return &workerv1.SegmentEffortImport{
		StartTime:       timestamppb.New(parseStravaTime(e.StartDate)),
		Elapsed:         durationpb.New(time.Duration(e.ElapsedTime) * time.Second),
		Moving:          durationpb.New(time.Duration(e.MovingTime) * time.Second),
		StartOffset:     clampI32(e.StartIndex),
		EndOffset:       clampI32(e.EndIndex),
		AvgHeartRateBpm: optI32FromFloat(e.AverageHeartrate),
		MaxHeartRateBpm: optI32FromFloat(e.MaxHeartrate),
		AvgPowerW:       optI32FromFloat(e.AverageWatts),
		AvgCadence:      optI32FromFloat(e.AverageCadence),
	}
}

// segmentActivityType maps Strava's segment activity_type ("Ride"/"Run") to the
// cairn enum. Strava segments are ride or run; default to ride.
func segmentActivityType(s string) cairnv1.ActivityType {
	switch strings.ToLower(s) {
	case "run":
		return cairnv1.ActivityType_ACTIVITY_TYPE_RUN
	default:
		return cairnv1.ActivityType_ACTIVITY_TYPE_RIDE
	}
}

// mapClimbCategory maps Strava's integer climb category (0..5) to the enum.
func mapClimbCategory(c int) *cairnv1.ClimbCategory {
	var v cairnv1.ClimbCategory
	switch c {
	case 1:
		v = cairnv1.ClimbCategory_CLIMB_CATEGORY_FOUR
	case 2:
		v = cairnv1.ClimbCategory_CLIMB_CATEGORY_THREE
	case 3:
		v = cairnv1.ClimbCategory_CLIMB_CATEGORY_TWO
	case 4:
		v = cairnv1.ClimbCategory_CLIMB_CATEGORY_ONE
	case 5:
		v = cairnv1.ClimbCategory_CLIMB_CATEGORY_HORS_CATEGORIE
	default:
		v = cairnv1.ClimbCategory_CLIMB_CATEGORY_NONE
	}
	return &v
}

func optF64(v float64) *float64 {
	if v == 0 {
		return nil
	}
	return &v
}

func optI32FromFloat(v float64) *int32 {
	if v == 0 {
		return nil
	}
	n := int32(v + 0.5)
	return &n
}

// parseStravaTime parses Strava's RFC3339 timestamps; zero time on error.
func parseStravaTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

// mapSummary maps the numeric Strava fields into the typed summary.
func mapSummary(a *stravaActivity) *cairnv1.ActivitySummary {
	s := &cairnv1.ActivitySummary{}
	if a.Distance > 0 {
		s.DistanceM = f64p(a.Distance)
	}
	if a.TotalElevationGain > 0 {
		s.ElevationGainM = f64p(a.TotalElevationGain)
	}
	if a.AverageSpeed > 0 {
		s.AvgSpeedMps = f64p(a.AverageSpeed)
	}
	if a.MaxSpeed > 0 {
		s.MaxSpeedMps = f64p(a.MaxSpeed)
	}
	if a.HasHeartrate {
		if a.AverageHeartrate > 0 {
			s.AvgHeartRateBpm = i32p(int32(a.AverageHeartrate))
		}
		if a.MaxHeartrate > 0 {
			s.MaxHeartRateBpm = i32p(int32(a.MaxHeartrate))
		}
	}
	if a.AverageWatts > 0 {
		s.AvgPowerW = i32p(int32(a.AverageWatts))
	}
	if a.MaxWatts > 0 {
		s.MaxPowerW = i32p(clampI32(a.MaxWatts))
	}
	if a.WeightedAverageWatts > 0 {
		s.NormalizedPowerW = i32p(int32(a.WeightedAverageWatts))
	}
	if a.AverageCadence > 0 {
		s.AvgCadence = i32p(int32(a.AverageCadence))
	}
	if a.AverageTemp != 0 {
		s.AvgTemperatureC = f64p(a.AverageTemp)
	}
	switch {
	case a.Calories > 0:
		s.CaloriesKcal = i32p(int32(a.Calories))
	case a.Kilojoules > 0:
		s.CaloriesKcal = i32p(int32(a.Kilojoules))
	}
	return s
}

// ---------------------------------------------------------------------------
// Column builders + small helpers
// ---------------------------------------------------------------------------

// latLngCols splits Strava's [lat, lon] pairs into dense columns of length n.
func latLngCols(streams stravaStreamsResponse, n int) (lat, lon []float64) {
	ch, ok := streams["latlng"]
	if !ok {
		return nil, nil
	}
	var latlngs [][2]float64
	if json.Unmarshal(ch.Data, &latlngs) != nil || len(latlngs) == 0 {
		return nil, nil
	}
	lat = make([]float64, n)
	lon = make([]float64, n)
	for i := 0; i < n && i < len(latlngs); i++ {
		lat[i], lon[i] = latlngs[i][0], latlngs[i][1]
	}
	return lat, lon
}

// floatCol decodes a Strava float column to a dense []float64 of length n.
func floatCol(streams stravaStreamsResponse, key string, n int) []float64 {
	ch, ok := streams[key]
	if !ok {
		return nil
	}
	var col []float64
	if err := json.Unmarshal(ch.Data, &col); err != nil || len(col) == 0 {
		return nil
	}
	out := make([]float64, n)
	for i := 0; i < n && i < len(col); i++ {
		out[i] = col[i]
	}
	return out
}

// intCol decodes a Strava integer column to a dense []int32 of length n.
func intCol(streams stravaStreamsResponse, key string, n int) []int32 {
	ch, ok := streams[key]
	if !ok {
		return nil
	}
	var col []float64
	if err := json.Unmarshal(ch.Data, &col); err != nil || len(col) == 0 {
		return nil
	}
	out := make([]int32, n)
	for i := 0; i < n && i < len(col); i++ {
		out[i] = int32(col[i])
	}
	return out
}

func f64p(v float64) *float64 { return &v }

// clampI32 narrows v to int32, saturating instead of wrapping.
func clampI32(v int) int32 {
	return int32(min(max(v, math.MinInt32), math.MaxInt32)) //nolint:gosec // G115: clamped to the int32 range
}
func i32p(v int32) *int32 { return &v }

// formatActivityID renders a Strava activity id as a string external_id.
func formatActivityID(id int64) string {
	return fmt.Sprintf("%d", id)
}
