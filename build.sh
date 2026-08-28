#!/bin/bash
set -euo pipefail
# Build the mentat images. Both binaries link statically against musl, so the
# artifacts image drops into a model image of any base.
#
#   VERSION=0.2.1 ./build.sh
#   DOCKER=docker ./build.sh          # if your daemon needs no sudo
#   REGISTRY=ghcr.io/mmastrac ./build.sh   # also tag for a registry
#
# Model images COPY --from mentat-artifacts:<ver>. Run this before those
# builds, or point them at mmastrac/mentat-artifacts:<ver> and skip it.
cd "$(dirname "$0")"
VERSION="${VERSION:-0.2.1}"
DOCKER="${DOCKER:-sudo docker}"
REGISTRY="${REGISTRY:-}"

$DOCKER build --target artifacts -t "mentat-artifacts:${VERSION}" .
$DOCKER build --target runtime   -t "mentatd:${VERSION}" .
$DOCKER build --target serve     -t "mentatd-serve:${VERSION}" .
$DOCKER build --target all       -t "mentat:${VERSION}" .

# What everything downstream depends on: the artifacts image carries both
# binaries and exactly one shim wheel, and the binaries are static.
$DOCKER run --rm "mentat-artifacts:${VERSION}" sh -c '
  set -e
  /out/mentatd --version
  /out/mentatd-serve --version
  ls /out/mentatd-*-py3-none-any.whl
  for b in /out/mentatd /out/mentatd-serve; do
    ! ldd "$b" 2>/dev/null | grep -q "=>" || { echo "$b is not static" >&2; exit 1; }
  done
  echo "both binaries static"
'

# `mentatd serve` only resolves where both binaries are installed.
$DOCKER run --rm "mentat:${VERSION}" serve --version

TAGS="mentat-artifacts:${VERSION} mentatd:${VERSION} mentatd-serve:${VERSION} mentat:${VERSION}"
if [ -n "$REGISTRY" ]; then
  for t in $TAGS; do
    $DOCKER tag "$t" "${REGISTRY}/${t}"
  done
  echo "tagged for ${REGISTRY}; push with:"
  for t in $TAGS; do echo "  $DOCKER push ${REGISTRY}/${t}"; done
fi

echo "TAGS=$(echo $TAGS | tr ' ' ',')"
