package capability

import "testing"

func TestManifest_Queries(t *testing.T) {
	m := Manifest{
		DataTypeActivity:      {Read: true, Backfill: true, Granularity: "per-activity"},
		DataTypeSegmentEffort: {Read: true, Backfill: true},
		DataTypeWeight:        {Read: true, Write: true, Granularity: "daily"},
	}

	if !m.CanRead(DataTypeActivity) || !m.SupportsBackfill(DataTypeActivity) {
		t.Error("Activity capability axes wrong")
	}
	if m.CanRead(DataTypeHRV) {
		t.Error("HRV absent should report not-readable")
	}
	if m.SupportsBackfill(DataTypeWeight) {
		t.Error("Weight should not report backfill")
	}
}

func TestManifest_ValidateRejectsUnknown(t *testing.T) {
	m := Manifest{DataType("Bogus"): {Read: true}}
	if err := m.Validate(); err == nil {
		t.Fatal("expected validation error for unknown type")
	}
	good := Manifest{DataTypeActivity: {Read: true}}
	if err := good.Validate(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDataType_Valid(t *testing.T) {
	if !DataTypeHRV.Valid() || DataType("nope").Valid() {
		t.Error("Valid wrong")
	}
}
