#!/usr/bin/env bash
# Regenerate the committed Go proto stubs from the Cairn proto contract.
#
# This repo is standalone (no Go dependency on cairn-core); the only shared
# contract is the proto messages + the NATS subjects. The generated stubs are
# committed under proto/ so the build needs no buf/build-base toolchain. Run
# this whenever the contract changes, then commit the result.
#
# Usage:
#   scripts/sync-proto.sh [path-to-cairn-core]   # default: ../cairn-core
#
# It copies the already-generated message stubs from cairn-core's gen/ (run
# `make proto` there first) and rewrites the single cross-package Go import in
# worker_service.pb.go to this module's path. The embedded rawDesc descriptors
# are left byte-for-byte intact (rewriting the go_package string inside them
# corrupts the length-prefixed descriptor).
set -euo pipefail

CORE="${1:-../cairn-core}"
MOD="github.com/johnnycube/cairn-provider-strava"
HERE="$(cd "$(dirname "$0")/.." && pwd)"

SRC_V1="$CORE/gen/proto/cairn/v1"
SRC_W="$CORE/gen/proto/cairn/worker/v1"
[ -d "$SRC_V1" ] || { echo "missing $SRC_V1 — run 'make proto' in cairn-core first" >&2; exit 1; }

echo "syncing proto stubs from $CORE"
cp "$SRC_V1"/*.pb.go "$HERE/proto/cairn/v1/"
cp "$SRC_W"/*.pb.go  "$HERE/proto/cairn/worker/v1/"

# Only worker_service.pb.go has a real cross-package Go import of cairn/v1.
# Anchor on the closing quote so the rawDesc go_package (…/cairn/worker/v1;workerv1)
# is never touched.
sed -i \
  "s#\"github.com/johnnycube/cairn-core/gen/proto/cairn/v1\"#\"$MOD/proto/cairn/v1\"#g" \
  "$HERE/proto/cairn/worker/v1/worker_service.pb.go"

( cd "$HERE" && go build ./... )
echo "proto stubs synced + build OK"
