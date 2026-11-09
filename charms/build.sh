#!/bin/bash
# Pack the four charms and push the service images to a registry the cluster
# can pull from (default: the MicroK8s registry addon on localhost:32000).
#
#   REGISTRY=localhost:32000 ./build.sh
#
# Images are tagged with the commit (TAG overrides it). A reused tag such as
# "dev" is not pulled again by a node that already has it, so a redeploy
# would silently run the old image.
set -euo pipefail
cd "$(dirname "$0")"
registry=${REGISTRY:-localhost:32000}
tag=${TAG:-$(git rev-parse --short=12 HEAD)}
for charm in fleetwire-api-k8s fleetwire-ingest-k8s fleetwire-bridge-k8s mosquitto-k8s; do
  (cd "$charm" && charmcraft pack)
done
for service in api ingest bridge; do
  docker build -q --build-arg "BINARY=fleetwire-$service" --build-arg "VERSION=$tag" \
    -t "$registry/fleetwire-$service:$tag" ..
  docker push -q "$registry/fleetwire-$service:$tag"
done
echo "images: $registry/fleetwire-{api,ingest,bridge}:$tag"
echo "for the integration test: export FLEETWIRE_IMAGES=$registry/fleetwire-{service}:$tag"
