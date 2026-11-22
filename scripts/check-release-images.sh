#!/usr/bin/env bash
# Check that the charms deploy the images a tag release publishes:
#
#   scripts/check-release-images.sh v0.1.2
#
# GoReleaser tags the fleetwire-api, -bridge and -ingest images with the
# tag's version without the "v" (0.1.2). Every reference to them in
# charms/bundle.yaml and in each charm's charmcraft.yaml (upstream-source)
# must name that version, so the bundle of a release deploys that release.
# The release workflow runs this first and publishes nothing on a mismatch.
set -euo pipefail
cd "$(dirname "$0")/.."
tag=${1:?usage: $0 TAG (for example v0.1.2)}
version=${tag#v}
status=0
for svc in api bridge ingest; do
  image="ghcr.io/ai-assistant-kira/fleetwire-$svc"
  for f in "charms/fleetwire-$svc-k8s/charmcraft.yaml" charms/bundle.yaml; do
    refs=$(grep -o "$image:[^[:space:]\"']*" "$f" || true)
    if [ -z "$refs" ]; then
      echo "$f: no $image image" >&2
      status=1
      continue
    fi
    while IFS= read -r ref; do
      if [ "$ref" != "$image:$version" ]; then
        echo "$f: $ref, but $tag publishes $image:$version" >&2
        status=1
      fi
    done <<<"$refs"
  done
done
if [ "$status" -eq 0 ]; then
  echo "the charms deploy the $version images"
fi
exit "$status"
