# Architecture

The Strava worker is a standalone process that imports a user's Strava data
into Cairn. It shares no Go code with cairn-core: the contract is the
`cairn.v1` / `cairn.worker.v1` proto messages (committed stubs in `proto/`) and
the NATS subjects.

## Components

```
cmd/worker-strava/   main, job handlers, Strava API client, auth, mapping, webhooks
internal/workersdk/  job loop, presence heartbeat, token cache, presigning, results
internal/nats/       JetStream bus adapter and the KV-backed rate limiter
internal/port/       interfaces the SDK depends on (JobBus, KV, RateLimiter, errors)
internal/capability/ capability manifest and data-type taxonomy
internal/inmem/      in-memory JobBus/KV for tests
proto/               generated contract stubs (scripts/sync-proto.sh)
```

`internal/` packages depend only on `port`; `cmd/worker-strava` wires the NATS
adapter, the rate limiter and the Strava auth handler into one
`workersdk.Worker`.

## Data flow

1. The worker connects to NATS. In production it presents an enrollment token;
   the server's auth-callout admits it with a scoped user JWT. Without a token
   (dev stack) it connects anonymously or with static credentials.
2. It pulls jobs from durable consumers on the `CAIRN_JOBS` stream and answers
   `cairn.discover.strava` by request/reply:
   - **discover** lists importable activity ids page by page, at most 25 pages
     per call; the server resumes from `next_page`.
   - **backfill** and **reconcile** walk `/athlete/activities` and enqueue one
     `fetch_source` job per activity. Reconcile only looks back to the later of
     the watermark minus an hour and 30 days, skips known ids and publishes the
     new watermark on an account-suffixed result subject.
   - **fetch_source** fetches the activity, its streams, photos and segment
     geometry, archives the raw JSON in the blob store, and publishes a
     `JobResult`. The result body is uploaded through a presigned URL and only
     an envelope crosses NATS (claim check), so result size is unbounded.
     Segments and efforts go in a second result so a rate limit mid-segments
     never fails the activity.
   - **parse_blob** re-maps an archived activity with the current mapping code,
     without spending Strava API budget.
3. Tokens come from the server (`cairn.tokens.strava.get`) and are cached;
   refreshes go to Strava's token endpoint with the per-user OAuth app
   credentials the server supplies, and the result is stored back.
4. A heartbeat publishes presence and build info to the `cairn_worker_presence`
   KV bucket, and the capability manifest to `cairn_worker_manifests` when it
   changes.

## Decisions

- **Shared rate limiter in NATS KV.** Every Strava call reserves from a daily
  and a 15-minute bucket via compare-and-swap on `cairn_rate_limits`, so all
  worker instances share one budget. Strava's usage headers resync the buckets
  after every response; a 429 anchors the bucket to the window that tripped.
  Exhaustion becomes `NakWithDelay`, and the pull loop pauses instead of
  churning redeliveries.
- **Error outcomes are typed.** Handlers return `TerminalError` (bad payload,
  revoked token: Term; fetch_source also publishes a failure result),
  `NakWithDelayError` (rate limit)
  or a plain error (transient: NAK with backoff). Long handlers extend the ack
  deadline so JetStream does not redeliver mid-processing.
- **Raw data is archived.** The fetched JSON is kept in the blob store, so a
  mapping change can be replayed with `parse_blob` without re-downloading from
  Strava.

## Build and release

`make check` is the definition of green and runs in CI on every push and pull
request. `.github/workflows/build.yml` gates on it, builds the image from the
`Dockerfile` on the shared `cairn-build-base` toolchain, pushes
`ghcr.io/johnnycube/cairn-provider-strava` from `main` and `v*` tags, attaches
SBOM and provenance, signs it keyless with cosign and attests it.
