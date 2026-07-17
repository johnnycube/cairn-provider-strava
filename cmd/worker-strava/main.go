// Command worker-strava is the cairn worker for Strava. It registers
// against a cairn-server via the enrollment-token flow, then consumes
// jobs on:
//
//	cairn.jobs.fetch_source.strava   — pull one activity (backfill/webhook/reconcile)
//	cairn.jobs.parse_blob.strava     — reparse a stored blob with new worker logic
//	cairn.jobs.backfill.strava       — list-paginate then fan out fetch_source jobs
//	cairn.jobs.reconcile.strava      — pull recent activities, enqueue fetch for missing
//
// Required ENV (defaults in workersdk.Config docs):
//
//	CAIRN_NATS_URL                       e.g. nats://nats:4222
//	CAIRN_WORKER_NAME                    must match enrollment WorkerNamePattern
//	CAIRN_WORKER_ENROLLMENT_TOKEN        from POST /admin/worker-enrollments
//	CAIRN_STRAVA_CLIENT_ID               for direct API auth (optional; usually
//	                                     unused — server fetches tokens via NATS)
//
// Worker version + package are compile-time constants (workerVersion /
// workerPackage) — they define the worker's schema, so they are not env-driven.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/johnnycube/cairn-provider-strava/internal/capability"
	"github.com/johnnycube/cairn-provider-strava/internal/workersdk"
)

// stravaManifest declares Strava's per-data-type capabilities — the schema-
// stable surface, identical across every deployment of this worker version. It
// carries no webhook/push flag: whether THIS instance owns the Strava webhook
// subject is a per-deployment property advertised separately in the presence
// heartbeat (set by registering WebhookEvent/WebhookVerify). Strava is
// read-only for Cairn (no write-back), supplies historical data (backfill), and
// yields activities, their laps, segment efforts, the segment definitions those
// efforts reference, gear, and provider-reported personal bests.
func stravaManifest() capability.Manifest {
	return capability.Manifest{
		capability.DataTypeActivity:      {Read: true, Backfill: true, Granularity: "per-activity"},
		capability.DataTypeLap:           {Read: true, Backfill: true},
		capability.DataTypeSegmentEffort: {Read: true, Backfill: true},
		capability.DataTypeSegment:       {Read: true, Backfill: true},
		capability.DataTypePersonalBest:  {Read: true, Backfill: true},
		capability.DataTypeGear:          {Read: true},
	}
}

const (
	workerName     = "strava-fetcher"
	workerProvider = "strava"
	jobStream      = "CAIRN_JOBS"

	// workerVersion + workerPackage are compile-time identity: together with the
	// capability manifest they define this worker's schema/contract. They are
	// baked into the binary (NOT env-overridable) — bump them in source when the
	// worker's output shape or version changes, then rebuild.
	workerVersion = "1"
	workerPackage = "cairns.internal.strava-importer"

	subjFetchSource = "cairn.jobs.fetch_source.strava"
	subjParseBlob   = "cairn.jobs.parse_blob.strava"
	subjBackfill    = "cairn.jobs.backfill.strava"
	subjReconcile   = "cairn.jobs.reconcile.strava"
	subjDiscover    = "cairn.discover.strava"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := run(logger); err != nil {
		logger.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	bus, err := connectBus(cfg, logger)
	if err != nil {
		return fmt.Errorf("connect bus: %w", err)
	}
	defer bus.Close()

	limiter := newRateLimiter(bus, logger)

	auth, err := NewStravaAuthHandler()
	if err != nil {
		return fmt.Errorf("init strava auth: %w", err)
	}

	w, err := workersdk.New(workersdk.Config{
		Name:       cfg.WorkerName,
		InstanceID: cfg.InstanceID,
		Version:    workerVersion,
		Package:    workerPackage,
		Provider:   workerProvider,
		Bus:        bus,
		Limiter:    limiter,
		Auth:       auth,
		JobStream:  jobStream,
		Logger:     logger,
		Manifest:   stravaManifest(),
	})
	if err != nil {
		return err
	}

	client := newStravaClient()
	// Sync the shared limiter to the budget Strava reports on every response —
	// authoritative capacities instead of the configured guesses, anchored to
	// Strava's real reset boundaries (quarter-hour / midnight UTC).
	client.onUsage = func(usedShort, limitShort, usedDaily, limitDaily int) {
		bg := context.Background()
		now := time.Now().UTC()
		_ = w.ObserveUsage(bg, "strava:short", usedShort, limitShort, nextQuarterHour(now))
		_ = w.ObserveUsage(bg, "strava:daily", usedDaily, limitDaily, nextMidnightUTC(now))
	}
	w.Handle(subjFetchSource, makeFetchSourceHandler(cfg, client))
	w.Handle(subjParseBlob, makeParseBlobHandler(cfg))
	w.Handle(subjBackfill, makeBackfillHandler(cfg, client))
	w.Handle(subjReconcile, makeReconcileHandler(cfg, client))

	// Webhook handlers — registered ONLY if CAIRN_STRAVA_WEBHOOK_VERIFY_TOKEN
	// is set. Without it, this worker instance doesn't claim Strava's
	// webhook subject and Strava verify requests will time out at the
	// server (so don't run multiple Strava workers with this token set;
	// only one should own the webhook subject).
	if os.Getenv("CAIRN_STRAVA_WEBHOOK_VERIFY_TOKEN") != "" {
		w.WebhookEvent(handleWebhookEvent)
		w.WebhookVerify(handleWebhookVerify)
		logger.Info("strava webhook handlers registered")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Discovery responder (request/reply): the Core asks what's importable.
	discSub, err := bus.RespondTo(ctx, subjDiscover, makeDiscoverHandler(w, client))
	if err != nil {
		return fmt.Errorf("register discover responder: %w", err)
	}
	defer func() { _ = discSub.Close(context.Background()) }()

	logger.Info("strava worker starting",
		"instance", cfg.InstanceID,
		"version", workerVersion,
		"handlers", []string{subjFetchSource, subjParseBlob, subjBackfill, subjReconcile},
	)

	return w.Run(ctx)
}
