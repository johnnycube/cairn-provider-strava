package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/opencairn-org/cairn-provider-strava/internal/port"
	"github.com/opencairn-org/cairn-provider-strava/internal/workersdk"
	workerv1 "github.com/opencairn-org/cairn-provider-strava/proto/cairn/worker/v1"
)

// ---------------------------------------------------------------------------
// import_gear.strava: the athlete's full gear list, retired included.
//
// GET /athlete lists bikes and shoes; GET /gear/{id} adds brand, model and
// description per item. A user has a handful of gear, so this stays well
// inside the budget. Published inline (no claim-check): the result is small.
// ---------------------------------------------------------------------------

type importGearJob struct {
	JobID     string `json:"job_id"`
	AccountID string `json:"account_id"`
	UserID    string `json:"user_id"`
	Provider  string `json:"provider"`
	Reason    string `json:"reason"`
}

func makeImportGearHandler(cfg workerConfig, client *stravaClient) func(context.Context, *workersdk.Worker, workersdk.Job) error {
	return func(ctx context.Context, w *workersdk.Worker, job workersdk.Job) error {
		var in importGearJob
		if err := json.Unmarshal(job.Body, &in); err != nil {
			return &port.TerminalError{Reason: "bad_payload", Cause: err}
		}
		if in.AccountID == "" {
			return &port.TerminalError{Reason: "missing_account_id"}
		}
		token, err := w.FetchToken(ctx, in.AccountID)
		if err != nil {
			return err
		}
		ref := fetchSourceJob{JobID: in.JobID, AccountID: in.AccountID, UserID: in.UserID}
		events, err := collectGear(ctx, w, client, token.AccessToken, ref)
		if err != nil {
			return mapStravaErr(ctx, w, cfg, job, ref, err, "fetch_gear")
		}

		body, err := protojson.Marshal(newJobResult(cfg, events...))
		if err != nil {
			return &port.TerminalError{Reason: "marshal_result", Cause: err}
		}
		w.Logger().Info("import_gear published", "account_id", in.AccountID, "gear", len(events))
		return w.Enqueue(ctx, "cairn.results.import_gear."+workerProvider, "result:"+in.JobID, body)
	}
}

// collectGear lists the athlete's bikes and shoes and emits one GEAR event
// per item, detail included.
func collectGear(ctx context.Context, w *workersdk.Worker, client *stravaClient, accessToken string, ref fetchSourceJob) ([]*workerv1.WorkerEvent, error) {
	if err := reserveRead(ctx, w, "strava:short"); err != nil {
		return nil, err
	}
	athlete, err := client.GetAthleteGear(ctx, accessToken)
	if err != nil {
		return nil, err
	}
	list := append(append([]stravaGearSummary(nil), athlete.Bikes...), athlete.Shoes...)
	events := make([]*workerv1.WorkerEvent, 0, len(list))
	for i := range list {
		g := &list[i]
		imp, err := gearDetail(ctx, w, client, accessToken, g)
		if err != nil {
			return nil, err
		}
		events = append(events, gearEvent(externalRef(ref, g.ID), imp))
	}
	return events, nil
}

// gearDetail fetches brand/model/description for one gear; a 404 (gear gone
// between the list and the detail call) falls back to the summary.
func gearDetail(ctx context.Context, w *workersdk.Worker, client *stravaClient, accessToken string, g *stravaGearSummary) (*workerv1.GearImport, error) {
	if err := reserveRead(ctx, w, "strava:short"); err != nil {
		return nil, err
	}
	detail, err := client.GetGear(ctx, accessToken, g.ID)
	switch {
	case errors.Is(err, ErrStravaNotFound):
		return mapGearSummary(g), nil
	case err != nil:
		return nil, fmt.Errorf("gear %s: %w", g.ID, err)
	}
	// The list carries the primary flag; the detail endpoint may not.
	detail.Primary = detail.Primary || g.Primary
	return mapGearDetail(detail), nil
}
