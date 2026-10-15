# Contributing to the Cairn Strava worker

Contributions are welcome. Open an issue first for non-trivial changes so the
direction is agreed before the work starts.

## License of contributions

The project is Apache-2.0. By submitting a contribution you agree it is
licensed under the same terms (Section 5 of the Apache License). There is no
CLA. Sign off your commits (`git commit -s`) to assert the
[Developer Certificate of Origin](https://developercertificate.org/).

## Setup

- Go (version in `go.mod`), `make`, and the pinned gate tools: `make tools`
  installs golangci-lint, govulncheck, go-licenses and fleetlint;
  [gitleaks](https://github.com/gitleaks/gitleaks) comes from its own release.
- Git hooks: `prek install` runs `.pre-commit-config.yaml` — hygiene, secret
  and workflow checks and conventional commit messages on commit, `make check`
  before push.
- A local Cairn dev stack (NATS) from cairn-core to run the worker; see the
  README.

## Working rules

- `make check` must pass: format, golangci-lint, `go vet`, tests with the race
  detector, the coverage floor, govulncheck, the license allowlist, a tidy
  module graph, gitleaks, the build and fleetlint. CI
  (`.github/workflows/check.yml`) runs the same target, and the image build
  gates on it.
- Commit messages follow Conventional Commits (`feat:`, `fix:`, `refactor:` …).
- `proto/` is generated from cairn-core's contract; do not edit it by hand.
  Regenerate with `scripts/sync-proto.sh ../cairn-core` and commit the result.
- Handlers map failures onto the SDK's three outcomes: `*port.TerminalError`
  (Term), `*port.NakWithDelayError` (rate limit) or any other error (NAK and
  retry). New handlers follow `docs/ARCHITECTURE.md`.
- User-facing changes get a CHANGELOG.md entry; `make release V=vX.Y.Z` refuses
  a tag without its section.
