# mentat: three build targets from one file, all arm64-native on gx10-n3.
#
#   --target artifacts  ->  mentat-artifacts:<ver>
#       /out/mentatd                        the static-ish daemon/CLI binary
#       /out/mentatd-*.whl                  the pure-Python `ray` shim
#       Consumed by the model images (home-infra's glm53/ds4-flash) via
#       COPY --from. Never runs.
#
#   --target runtime    ->  mentatd:<ver>
#       The host-level daemon container (see mentatd.yaml).
#
#   --target serve      ->  mentatd-serve:<ver>
#       The serving front door (see mentatd-serve.yaml). Its own crate under
#       serve/ -- tokio + hyper stay out of the daemon build on purpose.
#
# Build all with ./build.sh. The serving images hard-reference
# mentat-artifacts:<ver>, so build.sh must run on the same box first --
# there is no registry in this fleet.

# Moving major tag on purpose: the crate is std + serde + clap and builds on
# any modern stable; a broken toolchain bump fails HERE at build time, never
# at runtime.
ARG RUST_IMAGE=rust:1-slim-bookworm
FROM ${RUST_IMAGE} AS build
WORKDIR /src
COPY rust/ /src/
# --locked would need a committed Cargo.lock from the same cargo major; the
# assertion after the build is what actually gates the output.
RUN cargo build --release \
    && ls -l target/release/mentatd \
    && ./target/release/mentatd --version

# Wheel built under 3.12 to match the serving images' interpreter; the shim is
# pure python (py3-none-any), so this stage only pins the packaging toolchain.
FROM python:3.12-slim AS wheel
WORKDIR /src
COPY python/ /src/
RUN pip wheel --no-deps -w /dist . \
    && ls /dist/mentatd-*-py3-none-any.whl

FROM debian:bookworm-slim AS artifacts
COPY --from=build /src/target/release/mentatd /out/mentatd
COPY --from=wheel /dist/ /out/
RUN /out/mentatd --version && ls /out/mentatd-*-py3-none-any.whl

FROM debian:bookworm-slim AS runtime
COPY --from=build /src/target/release/mentatd /usr/local/bin/mentatd
RUN ln -s /usr/local/bin/mentatd /usr/local/bin/ray \
    && mentatd --version && ray --version
# 6379 control (ray-compatible RAY_ADDRESS port), 6380 http (/metrics /status
# /events). Runs under network_mode: host so EXPOSE is documentation.
EXPOSE 6379 6380
ENTRYPOINT ["mentatd"]
CMD ["daemon"]

FROM ${RUST_IMAGE} AS serve-build
WORKDIR /src
COPY serve/ /src/
RUN cargo build --release && ./target/release/mentatd-serve --version

FROM debian:bookworm-slim AS serve
COPY --from=serve-build /src/target/release/mentatd-serve /usr/local/bin/mentatd-serve
RUN mentatd-serve --version
# 6381: OpenAI-compatible /v1 plus the merged /mcp. 6382/udp: the daemons'
# announcement port it listens on. network_mode: host again, so EXPOSE is
# documentation.
EXPOSE 6381 6382/udp
ENTRYPOINT ["mentatd-serve"]
