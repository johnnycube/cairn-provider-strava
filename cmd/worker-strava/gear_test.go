package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/opencairn-org/cairn-provider-strava/internal/port"
	"github.com/opencairn-org/cairn-provider-strava/internal/workersdk"
	cairnv1 "github.com/opencairn-org/cairn-provider-strava/proto/cairn/v1"
	workerv1 "github.com/opencairn-org/cairn-provider-strava/proto/cairn/worker/v1"
)

func gearJob(in importGearJob) workersdk.Job {
	body, _ := json.Marshal(in)
	return workersdk.Job{Subject: subjImportGear, Body: body, DeliveryAttempt: 1}
}

func TestImportGear_PublishesEveryGear(t *testing.T) {
	h, _ := fetchHarness(t, &stravaAPI{})
	handler := makeImportGearHandler(workerConfig{WorkerName: workerName}, h.client)
	if err := handler(context.Background(), h.w, gearJob(importGearJob{JobID: "g1", AccountID: "acct", UserID: "u1"})); err != nil {
		t.Fatalf("import_gear: %v", err)
	}
	msgs := h.published("cairn.results.import_gear.strava")
	if len(msgs) != 1 {
		t.Fatalf("published %d results", len(msgs))
	}
	var res workerv1.JobResult
	if err := protojson.Unmarshal(msgs[0].Body, &res); err != nil {
		t.Fatal(err)
	}
	byID := map[string]*workerv1.GearImport{}
	for _, e := range res.GetEvents() {
		g := e.GetGear()
		if g == nil || g.GetRef().GetUserId() != "u1" || g.GetRef().GetExternalAccountId() != "acct" {
			t.Fatalf("event = %v", e)
		}
		byID[g.GetRef().GetExternalId()] = g.GetPayload()
	}
	if len(byID) != 3 {
		t.Fatalf("gear = %v; want bikes b1, b2 and shoes g1", byID)
	}
	// Detail adds brand/model; the list's primary flag is kept.
	if g := byID["b1"]; g.GetBrand() != "Canyon" || g.GetModel() != "Grail" || g.GetDescription() != "gravel" || !g.GetPrimary() || g.GetKind() != cairnv1.GearKind_GEAR_KIND_BIKE {
		t.Errorf("b1 = %v", g)
	}
	if g := byID["b2"]; !g.GetRetired() || g.GetName() != "Old" {
		t.Errorf("b2 = %v", g)
	}
	// Detail 404 falls back to the summary; shoes keyed by nickname.
	if g := byID["g1"]; g.GetKind() != cairnv1.GearKind_GEAR_KIND_SHOE || g.GetName() != "Pegasus" || !g.GetPrimary() {
		t.Errorf("g1 = %v", g)
	}
}

func TestImportGear_Errors(t *testing.T) {
	h, _ := fetchHarness(t, &stravaAPI{athlete: status(http.StatusInternalServerError)})
	handler := makeImportGearHandler(workerConfig{}, h.client)
	var term *port.TerminalError
	if err := handler(context.Background(), h.w, workersdk.Job{Body: []byte("{")}); !errors.As(err, &term) || term.Reason != "bad_payload" {
		t.Errorf("bad payload = %v", err)
	}
	if err := handler(context.Background(), h.w, gearJob(importGearJob{JobID: "g2"})); !errors.As(err, &term) || term.Reason != "missing_account_id" {
		t.Errorf("missing account = %v", err)
	}
	if err := handler(context.Background(), h.w, gearJob(importGearJob{JobID: "g3", AccountID: "acct"})); err == nil {
		t.Error("athlete 500: want an error (retry)")
	}
	if len(h.published("cairn.results.import_gear.strava")) != 0 {
		t.Error("nothing should be published on failure")
	}
}

func TestGearKindFromID(t *testing.T) {
	if gearKindFromID("b123") != cairnv1.GearKind_GEAR_KIND_BIKE || gearKindFromID("g1") != cairnv1.GearKind_GEAR_KIND_SHOE || gearKindFromID("x") != cairnv1.GearKind_GEAR_KIND_OTHER {
		t.Error("kind prefix mapping")
	}
}
