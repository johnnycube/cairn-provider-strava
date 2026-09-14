# Changelog

All notable changes to the Cairn Strava worker are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/), and the project aims
to follow [Semantic Versioning](https://semver.org/). Dates are ISO-8601.

## [0.2.4] — 2026-09-14

### Fixed
- `CAIRN_LOG_LEVEL` is honoured; the logger was hard-wired to info.
- Strava `invalid_client` and `unauthorized_client` token errors are reported
  as such instead of being classified as `invalid_grant`.

### Changed
- Webhook events with an unknown `aspect_type`, failed account lookups, and a
  missing rate-limiter KV bucket are logged instead of passing silently;
  backfill and reconcile log how many fetches they enqueued.
- Worker instance IDs default to a random hex id instead of a timestamp.
- The job and webhook loops share one ack/term/nak path, and photo mirroring
  reuses the presigned-upload helper.

### Removed
- Adapter code carried over from cairn-core that the worker never called:
  connection and TLS setup, stream bootstrap, the push-consumer path, the
  object store, unused KV and capability helpers, and the `CAIRN_NATS_*`
  cluster, credential, TLS, reconnect and retention config fields.
- `tokenCache.Invalidate`: a 401 already terminates the job as
  `needs_reauth`, and a re-fetch would return the same revoked token.

## [0.2.3] — 2026-09-11

### Changed
- Release images are signed keyless with cosign (Fulcio certificate bound to
  this repository's release workflow) and ship SLSA provenance and SBOM
  attestations; verify with `cosign verify` as documented in the workflow
  header. No functional change to the worker.
- Dependencies: `nats.go` 1.53.1, `protobuf` 1.36.12; GitHub Actions moved to
  the current majors of checkout, cache, and the docker build actions.

### Removed
- The Gitea mirror workflow — the repository is published from GitHub only.

## [0.2.2] — 2026-07-20

### Fixed
- GitHub Actions: the containerized test job now marks the checkout as a git
  `safe.directory`, fixing the "error obtaining VCS status" build failure that
  broke the v0.2.1 release pipeline. No code changes since 0.2.1 — this release
  exists to publish the images that release never produced.

## [0.2.1] — 2026-07-20

### Changed
- The worker re-publishes its manifest to `cairn_worker_manifests` every 5
  minutes (previously only at startup), keeping it fresh within the manifest
  bucket's new TTL so a live worker's manifest is never expired or reaped.
  Requires cairn-core ≥ v0.2.1.

## [0.2.0] — 2026-07-16

**First public release.** A standalone Go worker that imports Strava
activities into [Cairn](https://github.com/johnnycube/cairn-core) — read-only, no
write-back.

### Added
- Full activity import: activities with streams, laps, segment efforts,
  segment definitions, gear, and provider-reported personal bests.
- Cairn NATS control plane: enrollment-token registration, durable consumers
  for `fetch_source` / `parse_blob` / `backfill` / `reconcile` jobs, a
  `discover` request/reply endpoint, capability-manifest heartbeats to the
  worker-presence KV.
- Claim-checked results: event payloads upload to the blob store via presigned
  PUT and publish as small envelopes; terminal failures publish a fail-fast
  failure envelope with the true reason.
- Strava rate-limit handling driven by authoritative usage headers, with a
  reserved daily budget so reconcile can't starve user-driven imports.
- Webhook support and blob re-parse (`parse_blob`) so stored raw responses can
  be re-imported without spending API quota.
- No Go-code dependency on cairn-core: the shared contract is the committed
  proto stubs under `proto/` plus the NATS subjects.

## [0.1.0] — 2026-06-26

Initial development (internal pre-release iterations v0.1.0–v0.1.5).

[0.2.1]: https://github.com/johnnycube/cairn-provider-strava/releases/tag/v0.2.1
[0.2.0]: https://github.com/johnnycube/cairn-provider-strava/releases/tag/v0.2.0
