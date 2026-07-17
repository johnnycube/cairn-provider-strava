# Cairn Strava worker image.
#
# Build context is the repo root:
#   docker build -t cairn-provider-strava .
#
# The Go proto stubs are committed under proto/ (regenerated from the Cairn
# proto contract — see scripts/sync-proto.sh), so the build is a plain `go build`
# with NO buf / proto-generation step.
#
# The build stage uses the shared cairn-build-base toolchain image by default
# (pinned Go, matching cairn-core). Override BUILD_BASE to decouple from the
# core toolchain entirely, e.g.:
#   docker build --build-arg BUILD_BASE=golang:1.26-alpine -t cairn-provider-strava .
#
# A worker is a standalone process that joins the NATS control plane and
# consumes cairn.jobs.*.strava. In production it authenticates with an
# enrollment token (auth-callout); for the local dev stack it connects
# without one (see cmd/worker-strava/handlers.go connectBus).

ARG BUILD_BASE=ghcr.io/johnnycube/cairn-build-base:1

# ---- build stage ----------------------------------------------------------
FROM ${BUILD_BASE} AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ARG VERSION=docker
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /out/worker-strava ./cmd/worker-strava

# ---- runtime stage --------------------------------------------------------
FROM alpine:3.24

RUN apk add --no-cache ca-certificates && adduser -D -u 10002 worker

COPY --from=build /out/worker-strava /usr/local/bin/worker-strava

USER worker
ENTRYPOINT ["worker-strava"]
