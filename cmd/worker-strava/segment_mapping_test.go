package main

import (
	"testing"

	cairnv1 "github.com/johnnycube/cairn-provider-strava/proto/cairn/v1"
)

func TestMapSegmentToProto(t *testing.T) {
	d := &stravaDetailedSegment{
		stravaSegmentSummary: stravaSegmentSummary{
			Name:          "Test Climb",
			ActivityType:  "Ride",
			Distance:      1200,
			AverageGrade:  5.2,
			MaximumGrade:  9.1,
			ClimbCategory: 2, // → cat 3
			Starred:       true,
		},
		TotalElevationGain: 85,
	}
	d.Map.Polyline = "_p~iF~ps|U"

	s := mapSegmentToProto(d)
	if s.GetName() != "Test Climb" {
		t.Errorf("name = %q", s.GetName())
	}
	if s.GetActivityType() != cairnv1.ActivityType_ACTIVITY_TYPE_RIDE {
		t.Errorf("activity type = %v", s.GetActivityType())
	}
	if s.GetEncodedPolyline() != "_p~iF~ps|U" {
		t.Errorf("polyline = %q", s.GetEncodedPolyline())
	}
	if s.GetDistanceM() != 1200 {
		t.Errorf("distance = %v", s.GetDistanceM())
	}
	if s.ElevationGainM == nil || *s.ElevationGainM != 85 {
		t.Errorf("elevation gain = %v", s.ElevationGainM)
	}
	if s.ClimbCategory == nil || *s.ClimbCategory != cairnv1.ClimbCategory_CLIMB_CATEGORY_THREE {
		t.Errorf("climb category = %v", s.ClimbCategory)
	}
	if !s.GetStarred() {
		t.Errorf("starred should be true")
	}
}

func TestMapSegmentEffortToProto(t *testing.T) {
	e := &stravaSegmentEffort{
		ID:               999,
		ElapsedTime:      120,
		MovingTime:       118,
		StartDate:        "2026-06-01T10:00:00Z",
		StartIndex:       10,
		EndIndex:         50,
		AverageHeartrate: 165.4,
		AverageWatts:     250,
		AverageCadence:   0, // absent → nil
	}
	p := mapSegmentEffortToProto(e)
	if p.GetElapsed().AsDuration().Seconds() != 120 {
		t.Errorf("elapsed = %v", p.GetElapsed().AsDuration())
	}
	if p.GetStartOffset() != 10 || p.GetEndOffset() != 50 {
		t.Errorf("offsets = %d..%d", p.GetStartOffset(), p.GetEndOffset())
	}
	if p.AvgHeartRateBpm == nil || *p.AvgHeartRateBpm != 165 {
		t.Errorf("avg hr = %v (want 165, rounded)", p.AvgHeartRateBpm)
	}
	if p.AvgPowerW == nil || *p.AvgPowerW != 250 {
		t.Errorf("avg power = %v", p.AvgPowerW)
	}
	if p.AvgCadence != nil {
		t.Errorf("avg cadence should be nil when absent, got %v", *p.AvgCadence)
	}
	if p.GetStartTime().AsTime().IsZero() {
		t.Errorf("start time not parsed")
	}
}

func TestMapClimbCategory(t *testing.T) {
	cases := map[int]cairnv1.ClimbCategory{
		0: cairnv1.ClimbCategory_CLIMB_CATEGORY_NONE,
		1: cairnv1.ClimbCategory_CLIMB_CATEGORY_FOUR,
		5: cairnv1.ClimbCategory_CLIMB_CATEGORY_HORS_CATEGORIE,
	}
	for in, want := range cases {
		if got := mapClimbCategory(in); got == nil || *got != want {
			t.Errorf("mapClimbCategory(%d) = %v, want %v", in, got, want)
		}
	}
}
