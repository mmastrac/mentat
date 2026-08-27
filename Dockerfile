# mentat: four build targets from one file.
#
#   --target artifacts  ->  mentat-artifacts:<ver>
#       /out/mentatd                        the static daemon/CLI binary
#       /out/mentatd-serve                  the static router binary
#       /out/mentatd-*.whl                  the pure-Python `ray` shim
#       Consumed by the model images via COPY --from. Never runs.
#
#   --target runtime    ->  mentatd:<ver>
#       The host-level daemon container (see mentatd.yaml).
#
#   --target serve      ->  mentatd-serve:<ver>
#       The router (see mentatd-serve.yaml). Its own crate under serve/ --
#       tokio + hyper stay out of the daemon build on purpose.
#
#   --target all        ->  mentat:<ver>
#       Both binaries in one image, so `mentatd serve` resolves.
#
# Build all with ./build.sh. The serving images hard-reference
# mentat-artifacts:<ver>, so build.sh must run on the same box first --
# there is no registry in this fleet.

# Both binaries link statically against musl. The artifacts image is COPY'd
# into model images this repo does not control, and a static binary needs
# nothing from whatever base those images picked -- no glibc version to
# match, no loader to find. Neither binary needs a package on top of the
# base: interface enumeration is a libc call, not a call to `ip`.
ARG RUST_IMAGE=rust:1-alpine
ARG RUNTIME_IMAGE=alpine:3

FROM ${RUST_IMAGE} AS build
# musl-dev carries the libc the linker needs. The crate itself is
# std + serde + clap + hmac, so nothing else is required.
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY rust/ /src/
# --locked would need a committed Cargo.lock from the same cargo major; the
# assertions after the build are what actually gate the output.
RUN cargo build --release \
    && ./target/release/mentatd --version \
    && ! ldd target/release/mentatd 2>/dev/null | grep -q '=>' \
    && echo "mentatd is static"

FROM ${RUST_IMAGE} AS serve-build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY serve/ /src/
RUN cargo build --release \
    && ./target/release/mentatd-serve --version \
    && ! ldd target/release/mentatd-serve 2>/dev/null | grep -q '=>' \
    && echo "mentatd-serve is static"

# Wheel built under 3.12 to match the serving images' interpreter; the shim is
# pure python (py3-none-any), so this stage only pins the packaging toolchain.
FROM python:3.12-slim AS wheel
WORKDIR /src
COPY python/ /src/
RUN pip wheel --no-deps -w /dist . \
    && ls /dist/mentatd-*-py3-none-any.whl

FROM ${RUNTIME_IMAGE} AS artifacts
COPY --from=build /src/target/release/mentatd /out/mentatd
COPY --from=serve-build /src/target/release/mentatd-serve /out/mentatd-serve
COPY --from=wheel /dist/ /out/
RUN /out/mentatd --version && /out/mentatd-serve --version \
    && ls /out/mentatd-*-py3-none-any.whl

FROM ${RUNTIME_IMAGE} AS runtime
LABEL org.opencontainers.image.title="mentatd" \
      org.opencontainers.image.description="Minimal Ray replacement for vLLM multi-node serving" \
      org.opencontainers.image.source="https://github.com/mmastrac/mentat" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"
COPY --from=build /src/target/release/mentatd /usr/local/bin/mentatd
RUN ln -s /usr/local/bin/mentatd /usr/local/bin/ray \
    && mentatd --version && ray --version
# 6379 control (ray-compatible RAY_ADDRESS port), 6380 http (/metrics /status
# /events), 6382/udp announcements out. Runs under network_mode: host so
# EXPOSE is documentation.
EXPOSE 6379 6380 6382/udp
ENTRYPOINT ["mentatd"]
CMD ["daemon"]

FROM ${RUNTIME_IMAGE} AS serve
LABEL org.opencontainers.image.title="mentatd-serve" \
      org.opencontainers.image.description="OpenAI-compatible router and merged MCP for a mentat cluster" \
      org.opencontainers.image.source="https://github.com/mmastrac/mentat" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"
COPY --from=serve-build /src/target/release/mentatd-serve /usr/local/bin/mentatd-serve
RUN mentatd-serve --version
# 6381: OpenAI-compatible /v1 plus the merged /mcp. 6382/udp: the daemons'
# announcement port it listens on. network_mode: host again, so EXPOSE is
# documentation.
EXPOSE 6381 6382/udp
ENTRYPOINT ["mentatd-serve"]

# Both binaries together, which is what makes `mentatd serve` resolve: the
# dispatch looks for mentatd-serve beside mentatd first.
FROM ${RUNTIME_IMAGE} AS all
LABEL org.opencontainers.image.title="mentat" \
      org.opencontainers.image.description="mentatd and mentatd-serve in one image" \
      org.opencontainers.image.source="https://github.com/mmastrac/mentat" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"
COPY --from=build /src/target/release/mentatd /usr/local/bin/mentatd
COPY --from=serve-build /src/target/release/mentatd-serve /usr/local/bin/mentatd-serve
RUN ln -s /usr/local/bin/mentatd /usr/local/bin/ray \
    && mentatd --version && mentatd serve --version && ray --version
EXPOSE 6379 6380 6381 6382/udp
ENTRYPOINT ["mentatd"]
CMD ["daemon"]
