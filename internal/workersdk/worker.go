// Package workersdk provides the framework concrete cairn workers build
// on (Strava-fetcher, Garmin-fetcher, generic-FIT-parser, etc.). The SDK
// handles the boilerplate that's identical across providers:
//
//   - NATS connect with enrollment-token auth (worker generates an
//     ephemeral nkey, the server's auth-callout admits the connection
//     and mints a user-JWT scoped to the worker's permissions)
//   - Pull-consumer subscription per registered job subject
//   - Heartbeat loop that publishes presence to the cairn_worker_presence
//     KV bucket and the manifest to cairn_worker_manifests on change
//   - OAuth-token fetch via request/reply on cairn.tokens.<provider>.fetch,
//     with in-process caching that refreshes ahead of expiry
//   - Rate-limit reserve via the KV-backed port.RateLimiter before
//     external API calls
//   - Blob upload/download URL presigning via request/reply
//
// What the worker author writes:
//
//	func main() {
//	    w := workersdk.New(workersdk.Config{
//	        Name:    "strava-fetcher",
//	        Version: "v0.4.2",
//	        Bus:     bus, // a port.JobBus implementation
//	        Limiter: rl,  // a port.RateLimiter implementation
//	    })
//
//	    w.Handle("cairn.jobs.fetch_source.strava", handlers.FetchSource)
//	    w.Handle("cairn.jobs.parse_blob.strava", handlers.ParseBlob)
//	    w.Handle("cairn.jobs.backfill.strava", handlers.Backfill)
//	    w.Handle("cairn.jobs.reconcile.strava", handlers.Reconcile)
//
//	    if err := w.Run(context.Background()); err != nil {
//	        log.Fatal(err)
//	    }
//	}
//
// The HandlerFunc receives a Job with the body + headers + helpers
// for token-fetch and result-reporting. It returns nil for success
// (worker ACKs the message), an error for retry (NAK), or a wrapped
// *port.TerminalError for permanent failure (Term + DLQ).
package workersdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/johnnycube/cairn-provider-strava/internal/capability"
	"github.com/johnnycube/cairn-provider-strava/internal/port"
)

// ---------------------------------------------------------------------------
// Config + construction
// ---------------------------------------------------------------------------

// Config is the worker's startup configuration.
type Config struct {
	// Name is the worker's identity, matching the WorkerNamePattern on
	// the enrollment that admitted this connection. E.g. "strava-fetcher".
	Name string

	// InstanceID disambiguates multiple instances of the same Name
	// (e.g. "strava-fetcher-pod-7"). Defaults to a random UUID per
	// process start.
	InstanceID string

	// Version is the worker's binary version (a simple incrementing
	// integer, e.g. "1"). Reported in heartbeats + the manifest; the server
	// triggers update-available for older sources from the same package when
	// a higher version arrives. Part of the (provider, package, version)
	// compatibility contract.
	Version string

	// Provider is the external system this worker talks to ("strava",
	// "garmin", etc.). Used to scope rate-limiter buckets and token
	// fetches.
	Provider string

	// Package is the worker's stable build/package identity (e.g.
	// "cairns.internal.strava-importer"). With provider + version it forms
	// the compatibility contract the server keys update-available and
	// re-parse-eligibility on; the maintainer guarantees a given
	// (provider, package, version) triple parses identically.
	Package string

	// Bus is the JobBus the worker pulls jobs from and publishes results
	// to. In production, *natsadapter.Bus. In tests, *inmem.Bus.
	Bus port.JobBus

	// Limiter is consulted before every Provider API call. Pass nil
	// for workers that don't talk to a rate-limited external service
	// (e.g. internal parsing workers).
	Limiter port.RateLimiter

	// Auth is the worker's provider-specific OAuth refresh handler.
	// Pass nil for workers that don't need OAuth (e.g. an internal
	// parser worker reading from S3 with cairn-issued credentials).
	//
	// When non-nil, the SDK's token cache uses this for refresh when
	// the cached AccessToken expires.
	Auth AuthHandler

	// PullBatchSize is the number of jobs the worker fetches per Pull
	// cycle. Default 4 — bounds in-flight work per instance.
	PullBatchSize int

	// HeartbeatInterval is how often the worker stamps cairn_worker_presence
	// with a fresh last_seen. The KV bucket has a 60s TTL, so values
	// shorter than 30s give a 2x safety margin.
	HeartbeatInterval time.Duration

	// JobStream is the JetStream stream Pull-consumers attach to.
	// Defaults to "CAIRN_JOBS".
	JobStream string

	// TokenFetchTimeout caps per-call wait on the cairn.tokens.X.fetch
	// request/reply. Defaults to 5s.
	TokenFetchTimeout time.Duration

	// Logger is optional; falls back to slog.Default.
	Logger *slog.Logger

	// Manifest is the worker's per-data-type capability declaration
	// (read/write/backfill). Advertised in every heartbeat so the core
	// can route sync work and show the user which data types this provider
	// supplies — without the core knowing anything provider-specific. Empty
	// manifest = the worker declares no typed capabilities (legacy behaviour;
	// the coarse webhooks flag still works on its own).
	Manifest capability.Manifest
}

// Worker is the SDK entry point. Construct with New, register handlers
// with Handle, then call Run.
type Worker struct {
	cfg    Config
	logger *slog.Logger
	tokens *tokenCache

	// handlers maps job subject → registered handler.
	handlers map[string]HandlerFunc

	// lifecycle
	pulls    []port.PullSubscription
	wg       sync.WaitGroup
	stopCh   chan struct{}
	stopOnce sync.Once

	// webhooks holds optional WebhookEvent/WebhookVerify registrations
	// set by WebhookEvent() / WebhookVerify() before Run. Started by
	// startWebhookSubscribers from inside Run.
	webhooks webhookHandlers
}

// New constructs a worker. Returns an error only if the config is
// missing required fields.
func New(cfg Config) (*Worker, error) {
	if cfg.Name == "" {
		return nil, errors.New("workersdk: Config.Name required")
	}
	if cfg.Provider == "" {
		return nil, errors.New("workersdk: Config.Provider required")
	}
	if cfg.Bus == nil {
		return nil, errors.New("workersdk: Config.Bus required")
	}

	// Defaults.
	if cfg.InstanceID == "" {
		cfg.InstanceID = randID()
	}
	if cfg.PullBatchSize <= 0 {
		cfg.PullBatchSize = 4
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 20 * time.Second
	}
	if cfg.JobStream == "" {
		cfg.JobStream = "CAIRN_JOBS"
	}
	if cfg.TokenFetchTimeout <= 0 {
		cfg.TokenFetchTimeout = 5 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	w := &Worker{
		cfg:      cfg,
		logger:   cfg.Logger.With("worker", cfg.Name, "instance", cfg.InstanceID),
		handlers: map[string]HandlerFunc{},
		stopCh:   make(chan struct{}),
	}
	w.tokens = newTokenCache(cfg.Bus, cfg.Provider, cfg.Auth, cfg.TokenFetchTimeout, w.logger)
	return w, nil
}

// WorkerKey is this worker's composite identity {name}-{provider} (e.g.
// "worker1-strava") — the routing token for worker-owned NATS subjects and the
// webhook URL. Falls back to the provider alone when no name is configured.
func (w *Worker) WorkerKey() string {
	if w.cfg.Name == "" {
		return w.cfg.Provider
	}
	return w.cfg.Name + "-" + w.cfg.Provider
}

// ---------------------------------------------------------------------------
// Handler registration
// ---------------------------------------------------------------------------

// HandlerFunc is invoked per inbound job. Returning nil ACKs; returning
// a wrapped *port.TerminalError calls Term() (no more retries); returning
// a *port.NakWithDelayError NAKs with the specified delay; any other
// error NAKs with consumer-default backoff.
type HandlerFunc func(ctx context.Context, w *Worker, job Job) error

// Handle registers a HandlerFunc for a specific job subject. The subject
// is the durable consumer's filter; e.g. "cairn.jobs.fetch_source.strava".
//
// Must be called BEFORE Run. Calling Handle twice on the same subject
// panics — it's almost always a bug.
func (w *Worker) Handle(subject string, fn HandlerFunc) {
	if _, dup := w.handlers[subject]; dup {
		panic(fmt.Sprintf("workersdk: duplicate handler for %s", subject))
	}
	w.handlers[subject] = fn
}

// Job is the value HandlerFuncs receive. Body is typically a protobuf
// or JSON payload the handler decodes; headers carry routing metadata
// (Cairn-Reply-To, Cairn-Job-Id, Nats-Msg-Id).
type Job struct {
	Subject string
	Body    []byte
	Headers map[string]string

	// DeliveryAttempt is 1 on first delivery, 2+ on redelivery after a
	// NAK or AckWait expiry. Handlers can use this to detect retry
	// loops and Term() before MaxDeliver triggers.
	DeliveryAttempt int
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// Run starts the worker. Blocks until ctx is cancelled or an unrecoverable
// error occurs. Steps:
//
//  1. Publish initial heartbeat + manifest.
//  2. For each registered handler, create a pull-consumer subscription.
//  3. Launch the pull-loop goroutine per subscription.
//  4. Launch the heartbeat goroutine.
//  5. Wait for ctx.Done.
//  6. Drain in-flight handlers, publish final shutdown heartbeat, return.
func (w *Worker) Run(ctx context.Context) error {
	if len(w.handlers) == 0 && w.webhooks.event == nil && w.webhooks.verify == nil {
		return errors.New("workersdk: no handlers registered; call Handle/WebhookEvent/WebhookVerify before Run")
	}

	if err := w.publishHeartbeat(ctx); err != nil {
		w.logger.Warn("initial heartbeat failed", "error", err)
	}
	if err := w.publishManifest(ctx); err != nil {
		w.logger.Warn("initial manifest publish failed", "error", err)
	}

	for subject, fn := range w.handlers {
		ps, err := w.cfg.Bus.Pull(ctx, port.ConsumerConfig{
			Stream:        w.cfg.JobStream,
			Durable:       consumerName(w.cfg.Name, subject),
			Subject:       subject,
			DeliverPolicy: port.DeliverAll,
		})
		if err != nil {
			return fmt.Errorf("subscribe %s: %w", subject, err)
		}
		w.pulls = append(w.pulls, ps)
		w.wg.Add(1)
		go w.pullLoop(ctx, ps, subject, fn)
	}

	// Webhook subscribers (optional — only started if WebhookEvent or
	// WebhookVerify was called). Their lifecycle is the explicit Close
	// call below after ctx.Done.
	webhookSubs, err := w.startWebhookSubscribers(ctx)
	if err != nil {
		return err
	}

	w.wg.Add(1)
	go w.heartbeatLoop(ctx)

	<-ctx.Done()
	for _, s := range webhookSubs {
		_ = s.Close(context.Background())
	}
	return w.shutdown()
}

// Stop signals the worker to drain and exit. Safe to call multiple times.
func (w *Worker) Stop() {
	w.stopOnce.Do(func() { close(w.stopCh) })
}

func (w *Worker) shutdown() error {
	w.Stop()
	for _, p := range w.pulls {
		_ = p.Close(context.Background())
	}
	doneCh := make(chan struct{})
	go func() { w.wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(30 * time.Second):
		w.logger.Warn("shutdown: drain timeout exceeded; some goroutines still running")
	}
	w.logger.Info("worker stopped")
	return nil
}

// ---------------------------------------------------------------------------
// Pull loop
// ---------------------------------------------------------------------------

func (w *Worker) pullLoop(
	ctx context.Context,
	sub port.PullSubscription,
	subject string,
	fn HandlerFunc,
) {
	defer w.wg.Done()
	log := w.logger.With("subject", subject)
	log.Info("pull loop started")

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		default:
		}

		msgs, err := sub.Fetch(ctx, w.cfg.PullBatchSize)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			log.Warn("fetch failed", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// The batch is processed sequentially, and a single handler (slow
		// provider API, many photos/segments) can outlive the consumer's
		// AckWait. Without extending the ack deadline JetStream would
		// redeliver mid-processing — with unbounded MaxDeliver that becomes
		// an infinite refetch/reingest loop of the same job. Heartbeat every
		// message from fetch until its dispatch resolves it.
		stops := make([]func(), len(msgs))
		for i, m := range msgs {
			stops[i] = w.extendAckDeadline(ctx, m, subject)
		}

		// Track the largest rate-limit backoff seen in this batch. When the
		// provider (or our limiter) says "slow down", every message in the
		// batch is NAK-delayed AND the loop pauses before pulling more — so
		// we don't drain the stream into MaxAckPending-pinned redelivery churn.
		var backoff time.Duration
		for i, m := range msgs {
			d := w.dispatchMessage(ctx, m, subject, fn)
			stops[i]()
			if d > backoff {
				backoff = d
			}
		}
		if backoff > 0 {
			if backoff > maxPullBackoff {
				backoff = maxPullBackoff
			}
			log.Info("rate-limited; pausing pull loop", "backoff", backoff)
			select {
			case <-ctx.Done():
				return
			case <-w.stopCh:
				return
			case <-time.After(backoff):
			}
		}
	}
}

// maxPullBackoff caps how long the pull loop sleeps after a rate-limit hit, so
// ctx/stop stay responsive and the loop re-checks the limiter periodically even
// under a long provider window.
const maxPullBackoff = 60 * time.Second

// ackExtendInterval is how often an unresolved message's ack deadline is
// extended. Must be comfortably below the consumer's AckWait (2m).
const ackExtendInterval = 30 * time.Second

// extendAckDeadline keeps calling InProgress on the message until the returned
// stop func is invoked, so slow handlers (and messages queued behind them in
// the same batch) never hit AckWait-expiry redelivery. An InProgress error ends
// the loop — the message was already resolved or the delivery is gone, and
// re-arming a dead delivery only hides the redelivery that follows.
func (w *Worker) extendAckDeadline(ctx context.Context, m port.PullMessage, subject string) func() {
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(ackExtendInterval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if err := m.InProgress(ctx); err != nil {
					w.logger.Debug("ack-deadline extend failed", "subject", subject, "error", err)
					return
				}
			}
		}
	}()
	return func() { close(done) }
}

// dispatchMessage runs one handler and acks/naks/terms the message. It returns
// a non-zero backoff duration only when the handler reported a rate-limit
// (NakWithDelay) — the pull loop uses it to pause before fetching more.
func (w *Worker) dispatchMessage(
	ctx context.Context,
	m port.PullMessage,
	subject string,
	fn HandlerFunc,
) time.Duration {
	msg := m.Message()
	job := Job{
		Subject:         msg.Subject,
		Body:            msg.Body,
		Headers:         msg.Headers,
		DeliveryAttempt: msg.DeliveryAttempt,
	}

	err := fn(ctx, w, job)
	switch {
	case err == nil:
		if ackErr := m.Ack(ctx); ackErr != nil {
			w.logger.Warn("ack failed", "subject", subject, "error", ackErr)
		}
	default:
		var term *port.TerminalError
		var nakDelay *port.NakWithDelayError
		switch {
		case errors.As(err, &term):
			if e := m.Term(ctx); e != nil {
				w.logger.Warn("term failed", "subject", subject, "error", e)
			}
			w.logger.Info("handler returned terminal error",
				"subject", subject, "reason", term.Reason, "cause", err)
		case errors.As(err, &nakDelay):
			if e := m.NakWithDelay(ctx, nakDelay.Delay); e != nil {
				w.logger.Warn("nak-with-delay failed", "subject", subject, "error", e)
			}
			return nakDelay.Delay
		default:
			if e := m.Nak(ctx); e != nil {
				w.logger.Warn("nak failed", "subject", subject, "error", e)
			}
			w.logger.Info("handler returned retryable error",
				"subject", subject, "delivery", msg.DeliveryAttempt, "error", err)
		}
	}
	return 0
}

// ---------------------------------------------------------------------------
// Heartbeat + manifest
// ---------------------------------------------------------------------------

const (
	kvWorkerPresence  = "cairn_worker_presence"
	kvWorkerManifests = "cairn_worker_manifests"
)

func (w *Worker) heartbeatLoop(ctx context.Context) {
	defer w.wg.Done()
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-ticker.C:
			if err := w.publishHeartbeat(ctx); err != nil {
				w.logger.Debug("heartbeat publish failed", "error", err)
			}
		}
	}
}

type heartbeatPayload struct {
	WorkerName string `json:"worker_name"`
	// WorkerKey is the composite identity {name}-{provider} (e.g. worker1-strava)
	// — the routing token for worker-owned subjects and the webhook URL.
	WorkerKey  string `json:"worker_key"`
	InstanceID string `json:"instance_id"`
	Version    string `json:"version"`
	Provider   string `json:"provider"` // the service type the worker connects (e.g. "strava")
	Package    string `json:"package"`  // fully-qualified package/build identity
	// Build info, self-reported each heartbeat so the operator can see exactly
	// what binary is running. Sourced from runtime/debug build info.
	GoVersion string `json:"go_version,omitempty"`
	Commit    string `json:"commit,omitempty"`
	BuildDate string `json:"build_date,omitempty"`
	// Webhooks advertises that THIS worker owns its webhook subject (it
	// registered WebhookEvent/WebhookVerify handlers). The server surfaces the
	// webhook URL only when a worker advertises it — the core has no per-provider
	// webhook knowledge of its own.
	Webhooks bool `json:"webhooks"`
	// Capabilities is the per-data-type capability manifest (read/write/
	// backfill). Provider-neutral; the core reads it generically. Omitted for
	// workers that declare no manifest.
	Capabilities capability.Manifest `json:"capabilities,omitempty"`
	LastSeen     time.Time           `json:"last_seen"`
}

// workerBuildInfo reads the binary's embedded build metadata once. Returns the
// Go toolchain version, VCS commit (short), and VCS build time when available
// (they are when built normally; blank under `go test`/`go run` without VCS).
func workerBuildInfo() (goVersion, commit, buildDate string) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return runtime.Version(), "", ""
	}
	goVersion = bi.GoVersion
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			commit = s.Value
			if len(commit) > 12 {
				commit = commit[:12]
			}
		case "vcs.time":
			buildDate = s.Value
		}
	}
	return goVersion, commit, buildDate
}

func (w *Worker) publishHeartbeat(ctx context.Context) error {
	kv, err := w.cfg.Bus.KV(kvWorkerPresence)
	if err != nil {
		return err
	}
	goVer, commit, buildDate := workerBuildInfo()
	payload, err := json.Marshal(heartbeatPayload{
		WorkerName:   w.cfg.Name,
		WorkerKey:    w.WorkerKey(),
		InstanceID:   w.cfg.InstanceID,
		Version:      w.cfg.Version,
		Provider:     w.cfg.Provider,
		Package:      w.cfg.Package,
		GoVersion:    goVer,
		Commit:       commit,
		BuildDate:    buildDate,
		Webhooks:     w.webhooks.event != nil || w.webhooks.verify != nil,
		Capabilities: w.cfg.Manifest,
		LastSeen:     time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	key := w.cfg.Name + "." + w.cfg.InstanceID
	_, err = kv.Put(ctx, key, payload)
	return err
}

type manifestPayload struct {
	WorkerName string `json:"worker_name"`
	Version    string `json:"version"`
	// Provider + Package drive the update-available trigger: a newer worker
	// from the same package for the same provider marks older sources stale,
	// independent of the routing name/alias.
	Provider  string    `json:"provider"`
	Package   string    `json:"package"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (w *Worker) publishManifest(ctx context.Context) error {
	kv, err := w.cfg.Bus.KV(kvWorkerManifests)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(manifestPayload{
		WorkerName: w.cfg.Name,
		Version:    w.cfg.Version,
		Provider:   w.cfg.Provider,
		Package:    w.cfg.Package,
		UpdatedAt:  time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	_, err = kv.Put(ctx, w.cfg.Name, payload)
	return err
}

// ---------------------------------------------------------------------------
// Handler helpers
// ---------------------------------------------------------------------------

// FetchToken returns a valid OAuth access token for the given external
// account. Cached in-process; refreshed from the server before expiry.
// Returns a wrapped *port.TerminalError when the server reports the
// account needs reauth.
func (w *Worker) FetchToken(ctx context.Context, accountID string) (Token, error) {
	return w.tokens.Get(ctx, accountID)
}

// ReserveAPI reserves `tokens` against the named rate-limit bucket. Returns
// a *port.NakWithDelayError with the right delay if the bucket is exhausted
// — handlers should `return err` to NAK the job with that delay.
//
// Bucket name conventions: "strava:short", "strava:daily", "garmin:short".
// Returning nil means proceed with the API call.
func (w *Worker) ReserveAPI(ctx context.Context, bucket string, tokens int) error {
	if w.cfg.Limiter == nil {
		return nil
	}
	ok, retryAfter, err := w.cfg.Limiter.Reserve(ctx, bucket, tokens)
	if err != nil {
		return fmt.Errorf("rate limiter reserve %s: %w", bucket, err)
	}
	if !ok {
		return &port.NakWithDelayError{
			Reason: "rate_limited",
			Delay:  retryAfter,
		}
	}
	return nil
}

// ObserveRateLimit429 hard-syncs the bucket state after the upstream
// API returns 429. `available` is what the provider's response headers
// report; `windowResetsAt` is the absolute time their window resets.
// Subsequent ReserveAPI calls reflect this state.
func (w *Worker) ObserveRateLimit429(ctx context.Context, bucket string, available int, windowResetsAt time.Time) error {
	if w.cfg.Limiter == nil {
		return nil
	}
	return w.cfg.Limiter.ForceRefill(ctx, bucket, available, windowResetsAt)
}

// ObserveUsage syncs a bucket to the provider-reported (used, limit) pair
// from response headers — authoritative accounting on every API call, so
// the local limiter never drifts from the provider's real budget.
func (w *Worker) ObserveUsage(ctx context.Context, bucket string, used, limit int, windowResetsAt time.Time) error {
	if w.cfg.Limiter == nil {
		return nil
	}
	return w.cfg.Limiter.SyncUsage(ctx, bucket, used, limit, windowResetsAt)
}

// PublishResult sends the job's result on the Cairn-Reply-To subject
// from the original message. msgID populates Nats-Msg-Id for dedup.
//
// If the job's headers don't include a reply subject, falls back to
// cairn.results.<derived-from-job-subject>.
func (w *Worker) PublishResult(ctx context.Context, job Job, msgID string, body []byte) error {
	reply := job.Headers["Cairn-Reply-To"]
	if reply == "" {
		reply = deriveResultSubject(job.Subject)
	}
	return w.cfg.Bus.Publish(ctx, reply, msgID, body)
}

// PresignUpload requests a presigned URL from the server for uploading
// raw provider data. Returns the URL + required headers + expiry.
func (w *Worker) PresignUpload(ctx context.Context, in PresignUploadRequest) (PresignedURL, error) {
	if in.Provider == "" {
		in.Provider = w.cfg.Provider
	}
	body, err := json.Marshal(in)
	if err != nil {
		return PresignedURL{}, err
	}
	subj := "cairn.blobs.presign_upload." + w.cfg.Provider
	resp, err := w.cfg.Bus.Request(ctx, subj, body, 3*time.Second)
	if err != nil {
		return PresignedURL{}, fmt.Errorf("presign upload: %w", err)
	}
	var out PresignedURL
	if err := json.Unmarshal(resp, &out); err != nil {
		return PresignedURL{}, fmt.Errorf("decode presign reply: %w", err)
	}
	return out, nil
}

// PresignDownload requests a presigned URL for reading a raw blob (used
// in the parse_blob.<provider> flow when reparsing from S3).
func (w *Worker) PresignDownload(ctx context.Context, in PresignDownloadRequest) (PresignedURL, error) {
	body, err := json.Marshal(in)
	if err != nil {
		return PresignedURL{}, err
	}
	subj := "cairn.blobs.presign_download." + w.cfg.Provider
	resp, err := w.cfg.Bus.Request(ctx, subj, body, 3*time.Second)
	if err != nil {
		return PresignedURL{}, fmt.Errorf("presign download: %w", err)
	}
	var out PresignedURL
	if err := json.Unmarshal(resp, &out); err != nil {
		return PresignedURL{}, fmt.Errorf("decode presign reply: %w", err)
	}
	return out, nil
}

// Enqueue publishes a new job on a JobBus subject. Used by handlers
// that fan out (e.g. backfill paginates a provider's activity list,
// then enqueues a fetch_source.<provider> job per activity).
//
// msgID populates Nats-Msg-Id so retries collapse within the dedup window.
func (w *Worker) Enqueue(ctx context.Context, subject, msgID string, body []byte) error {
	return w.cfg.Bus.Publish(ctx, subject, msgID, body)
}

// Request performs a NATS request/reply round-trip. Used by handlers
// that need synchronous server-side data (e.g. webhook handlers
// looking up an account by provider+external_id via
// cairn.accounts.lookup_by_provider_ext).
//
// For OAuth token fetches, handlers should use FetchToken instead —
// it goes through the dedicated cache.
func (w *Worker) Request(
	ctx context.Context,
	subject string,
	body []byte,
	timeout time.Duration,
) ([]byte, error) {
	return w.cfg.Bus.Request(ctx, subject, body, timeout)
}

// PublishEvent publishes a domain event on cairn.events.<topic>.
// Convenience wrapper around Bus.Publish with msgID derived from the
// event payload.
func (w *Worker) PublishEvent(ctx context.Context, topic string, msgID string, body []byte) error {
	return w.cfg.Bus.Publish(ctx, "cairn.events."+topic, msgID, body)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// consumerName builds the durable consumer name for a worker+subject
// pair. Format: "<worker>__<subject-with-dots-replaced>".
func consumerName(worker, subject string) string {
	clean := ""
	for _, r := range subject {
		switch {
		case r == '.':
			clean += "_"
		case r == '*' || r == '>':
			// strip wildcards from durable names
		default:
			clean += string(r)
		}
	}
	return worker + "__" + clean
}

// deriveResultSubject converts a job subject into the canonical result
// subject. E.g. "cairn.jobs.fetch_source.strava" → "cairn.results.fetch_source.strava".
// Logger returns the worker's structured logger so handlers can log.
func (w *Worker) Logger() *slog.Logger { return w.logger }

func deriveResultSubject(jobSubject string) string {
	const jobsPrefix = "cairn.jobs."
	if len(jobSubject) > len(jobsPrefix) && jobSubject[:len(jobsPrefix)] == jobsPrefix {
		return "cairn.results." + jobSubject[len(jobsPrefix):]
	}
	return jobSubject + ".result"
}

// randID generates a short random identifier for the worker instance.
// Deliberately not crypto-strong — collisions are operational, not
// security, concerns.
func randID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), time.Now().Nanosecond())
}

// PresignUploadRequest matches the JSON shape the cairn-server's
// presign-upload handler expects.
type PresignUploadRequest struct {
	SourceID string `json:"source_id,omitempty"`
	UserID   string `json:"user_id,omitempty"`
	// Provider is auto-populated by PresignUpload from Worker.cfg.Provider —
	// callers don't need to set it. The server uses it as the blob-key
	// prefix segment.
	Provider string `json:"provider,omitempty"`
	// Kind selects the server-side key prefix: "" for durable raw
	// archives, "result" for claim-checked JobResult bodies (short-lived,
	// server deletes after ingest).
	Kind          string `json:"kind,omitempty"`
	ContentType   string `json:"content_type"`
	ContentLength int64  `json:"content_length"`
	ContentSHA256 string `json:"content_sha256,omitempty"`
}

// PresignDownloadRequest matches the presign-download handler.
type PresignDownloadRequest struct {
	BlobID string `json:"blob_id,omitempty"`
	Handle string `json:"handle,omitempty"` // for fallback_handle exchange
}

// PresignedURL matches the response shape from both presign endpoints.
type PresignedURL struct {
	Method          string            `json:"method"`
	URL             string            `json:"url"`
	BlobID          string            `json:"blob_id,omitempty"`
	ExpiresAt       time.Time         `json:"expires_at"`
	RequiredHeaders map[string]string `json:"required_headers,omitempty"`
}
