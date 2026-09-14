// Package capability defines the provider-neutral data-type taxonomy and the
// per-type capability model (read/write/backfill) a worker advertises.
// Pure domain: providers map their schema onto these types; the core queries
// capabilities generically.
package capability

// DataType is a provider-neutral data category. String values are the stable
// wire identifiers (manifests, heartbeat, proto) — keep them stable.
type DataType string

// Activity-related types. PersonalBest (not "best effort", which means delivery)
// is a provider-reported best like "fastest 5k".
const (
	DataTypeActivity      DataType = "Activity"
	DataTypeLap           DataType = "Lap"
	DataTypeSegmentEffort DataType = "SegmentEffort"
	DataTypePersonalBest  DataType = "PersonalBest"
)

// Time-series / health metrics.
const (
	DataTypeSteps       DataType = "Steps"
	DataTypeHRV         DataType = "HRV"
	DataTypeRestingHR   DataType = "RestingHR"
	DataTypeSleep       DataType = "Sleep"
	DataTypeWeight      DataType = "Weight"
	DataTypeWaterIntake DataType = "WaterIntake"
)

// Static / reference types. Segment is the definition, distinct from SegmentEffort.
const (
	DataTypeSegment        DataType = "Segment"
	DataTypeGear           DataType = "Gear"
	DataTypeAthleteProfile DataType = "AthleteProfile"
)

// known is the closed set of data types the core understands. The worker only
// needs membership; labels and categories live in cairn-core.
var known = map[DataType]struct{}{
	DataTypeActivity:       {},
	DataTypeLap:            {},
	DataTypeSegmentEffort:  {},
	DataTypePersonalBest:   {},
	DataTypeSteps:          {},
	DataTypeHRV:            {},
	DataTypeRestingHR:      {},
	DataTypeSleep:          {},
	DataTypeWeight:         {},
	DataTypeWaterIntake:    {},
	DataTypeSegment:        {},
	DataTypeGear:           {},
	DataTypeAthleteProfile: {},
}

// Valid reports whether dt is a known canonical data type.
func (dt DataType) Valid() bool {
	_, ok := known[dt]
	return ok
}
