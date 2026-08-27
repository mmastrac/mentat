#!/bin/bash
set -euo pipefail
# Build both mentat images natively on a GX10 (run on gx10-n3, the box that
# is NOT part of the serving pair, so a rebuild never disturbs a live cluster).
#
#   VERSION=0.1.0 ./build.sh
#
# glm53/Dockerfile and ds4-flash/Dockerfile COPY --from mentat-artifacts:<ver>,
# so this must have run on the build box before either of those builds.
cd "$(dirname "$0")"
VERSION="${VERSION:-0.1.0}"

sudo docker build --target artifacts -t "mentat-artifacts:${VERSION}" .
sudo docker build --target runtime   -t "mentatd:${VERSION}" .
sudo docker build --target serve     -t "mentat-serve:${VERSION}" .

# The one property everything downstream depends on: the artifacts image
# carries the binary and exactly one shim wheel.
sudo docker run --rm "mentat-artifacts:${VERSION}" sh -c \
  '/out/mentat --version && ls /out/mentat_ray_shim-*-py3-none-any.whl'

echo "BUILD_EXIT=$? TAGS=mentat-artifacts:${VERSION},mentatd:${VERSION},mentat-serve:${VERSION}"
