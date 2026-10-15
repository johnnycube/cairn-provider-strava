# Security policy

## Supported versions

The latest release (the newest `v*` tag and its
`ghcr.io/johnnycube/cairn-provider-strava` image). Older releases do not
receive fixes.

## Reporting a vulnerability

Please do not open a public issue. Report suspected vulnerabilities privately
through GitHub's "Report a vulnerability" button on the Security tab, or by
email to `security@johanneskueber.com`. You will get an acknowledgement within
7 days and a fix or a decision within 30.

Vulnerabilities in the Cairn server belong to
[cairn-core](https://github.com/johnnycube/cairn-core); vulnerabilities in
Strava's services go to Strava.

## Verifying images

Published images are signed keyless with cosign and carry SBOM and provenance
attestations; the header of `.github/workflows/build.yml` has the
`cosign verify` command.
