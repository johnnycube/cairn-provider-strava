package main

import (
	"encoding/json"
	"fmt"
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
		lap := &cairnv1.ActivityLap{
			Index:           int32(l.LapIndex),
			Label:           l.Name,
			StartTime:       timestamppb.New(parseStravaTime(l.StartDate)),
			ElapsedDuration: durationpb.New(time.Duration(l.ElapsedTime) * time.Second),
			MovingDuration:  durationpb.New(time.Duration(l.MovingTime) * time.Second),
		}
		if l.Distance > 0 {
			lap.DistanceM = &l.Distance
		}
		if l.AverageSpeed > 0 {
			lap.AvgSpeedMps = &l.AverageSpeed
		}
		if l.AverageHeartrate > 0 {
			lap.AvgHeartRateBpm = optI32FromFloat(l.AverageHeartrate)
		}
		if l.MaxHeartrate > 0 {
			lap.MaxHeartRateBpm = optI32FromFloat(l.MaxHeartrate)
		}
		if l.AverageWatts > 0 {
			lap.AvgPowerW = optI32FromFloat(l.AverageWatts)
		}
		if l.AverageCadence > 0 {
			lap.AvgCadence = optI32FromFloat(l.AverageCadence)
		}
		if l.TotalElevationGain > 0 {
			lap.ElevationGainM = &l.TotalElevationGain
		}
		out = append(out, lap)
	}
	return out
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
		SampleCount:  int32(n),
		ResolutionHz: 1.0,
		TimeS:        seconds,
	}
	var channels []cairnv1.StreamChannel

	// latlng is a [N][2] of [lat, lon] pairs in Strava's response.
	if ch, ok := streams["latlng"]; ok {
		var latlngs [][2]float64
		if json.Unmarshal(ch.Data, &latlngs) == nil && len(latlngs) > 0 {
			lat := make([]float64, n)
			lon := make([]float64, n)
			for i := 0; i < n && i < len(latlngs); i++ {
				lat[i], lon[i] = latlngs[i][0], latlngs[i][1]
			}
			st.Latitude, st.Longitude = lat, lon
			channels = append(channels,
				cairnv1.StreamChannel_STREAM_CHANNEL_LATITUDE,
				cairnv1.StreamChannel_STREAM_CHANNEL_LONGITUDE)
		}
	}

	if col := floatCol(streams, "altitude", n); col != nil {
		st.AltitudeM = col
		channels = append(channels, cairnv1.StreamChannel_STREAM_CHANNEL_ALTITUDE)
	}
	if col := floatCol(streams, "distance", n); col != nil {
		st.DistanceM = col
		channels = append(channels, cairnv1.StreamChannel_STREAM_CHANNEL_DISTANCE)
	}
	if col := floatCol(streams, "velocity_smooth", n); col != nil {
		st.SpeedMps = col
		channels = append(channels, cairnv1.StreamChannel_STREAM_CHANNEL_SPEED)
	}
	if col := intCol(streams, "heartrate", n); col != nil {
		st.HeartRateBpm = col
		channels = append(channels, cairnv1.StreamChannel_STREAM_CHANNEL_HEART_RATE)
	}
	if col := intCol(streams, "watts", n); col != nil {
		st.PowerW = col
		channels = append(channels, cairnv1.StreamChannel_STREAM_CHANNEL_POWER)
	}
	if col := intCol(streams, "cadence", n); col != nil {
		st.Cadence = col
		channels = append(channels, cairnv1.StreamChannel_STREAM_CHANNEL_CADENCE)
	}
	if col := floatCol(streams, "temp", n); col != nil {
		st.TemperatureC = col
		channels = append(channels, cairnv1.StreamChannel_STREAM_CHANNEL_TEMPERATURE)
	}
	if col := floatCol(streams, "grade_smooth", n); col != nil {
		st.Grade = col
		channels = append(channels, cairnv1.StreamChannel_STREAM_CHANNEL_GRADE)
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
	switch src {
	case "Ride":
		return cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_ROAD, false, false
	case "GravelRide":
		return cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_GRAVEL, false, false
	case "MountainBikeRide":
		return cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_MTB, false, false
	case "EMountainBikeRide":
		return cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_MTB, true, false
	case "EBikeRide":
		return cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_ROAD, true, false
	case "VirtualRide":
		return cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_ROAD, false, true
	case "TrackRide", "Velomobile":
		return cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_TRACK, false, false

	case "Run":
		return cairnv1.ActivityType_ACTIVITY_TYPE_RUN, cairnv1.Discipline_DISCIPLINE_RUN_ROAD, false, false
	case "TrailRun":
		return cairnv1.ActivityType_ACTIVITY_TYPE_RUN, cairnv1.Discipline_DISCIPLINE_RUN_TRAIL, false, false
	case "VirtualRun":
		return cairnv1.ActivityType_ACTIVITY_TYPE_RUN, cairnv1.Discipline_DISCIPLINE_RUN_ROAD, false, true

	case "Hike":
		return cairnv1.ActivityType_ACTIVITY_TYPE_HIKE, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false
	case "Walk":
		return cairnv1.ActivityType_ACTIVITY_TYPE_WALK, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false

	case "Swim":
		return cairnv1.ActivityType_ACTIVITY_TYPE_SWIM, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false

	case "AlpineSki":
		return cairnv1.ActivityType_ACTIVITY_TYPE_SKI, cairnv1.Discipline_DISCIPLINE_SKI_ALPINE, false, false
	case "NordicSki", "RollerSki":
		return cairnv1.ActivityType_ACTIVITY_TYPE_SKI, cairnv1.Discipline_DISCIPLINE_SKI_NORDIC, false, false
	case "BackcountrySki":
		return cairnv1.ActivityType_ACTIVITY_TYPE_SKI, cairnv1.Discipline_DISCIPLINE_SKI_BACKCOUNTRY, false, false

	case "Rowing", "VirtualRow":
		return cairnv1.ActivityType_ACTIVITY_TYPE_ROW, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false

	// Extended sport set (Strava sport_type names → coarse Cairn types; no
	// disciplines defined for these yet).
	case "Snowboard":
		return cairnv1.ActivityType_ACTIVITY_TYPE_SNOWBOARD, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false
	case "Kayaking", "Canoeing":
		return cairnv1.ActivityType_ACTIVITY_TYPE_KAYAK, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false
	case "StandUpPaddling":
		return cairnv1.ActivityType_ACTIVITY_TYPE_SUP, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false
	case "Surfing", "Windsurf", "Kitesurf":
		return cairnv1.ActivityType_ACTIVITY_TYPE_SURF, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false
	case "Golf":
		return cairnv1.ActivityType_ACTIVITY_TYPE_GOLF, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false
	case "RockClimbing":
		return cairnv1.ActivityType_ACTIVITY_TYPE_CLIMB, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false
	case "InlineSkate", "IceSkate", "Skateboard":
		return cairnv1.ActivityType_ACTIVITY_TYPE_SKATE, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false
	case "Elliptical":
		return cairnv1.ActivityType_ACTIVITY_TYPE_ELLIPTICAL, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false
	case "Wheelchair", "Handcycle":
		return cairnv1.ActivityType_ACTIVITY_TYPE_WHEELCHAIR, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false
	case "Tennis", "Pickleball", "Squash", "Badminton", "Racquetball", "TableTennis":
		return cairnv1.ActivityType_ACTIVITY_TYPE_TENNIS, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false
	}
	return cairnv1.ActivityType_ACTIVITY_TYPE_WORKOUT, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED, false, false
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
		StartOffset:     int32(e.StartIndex),
		EndOffset:       int32(e.EndIndex),
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
		s.MaxPowerW = i32p(int32(a.MaxWatts))
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
func i32p(v int32) *int32     { return &v }

// formatActivityID renders a Strava activity id as a string external_id.
func formatActivityID(id int64) string {
	return fmt.Sprintf("%d", id)
}
