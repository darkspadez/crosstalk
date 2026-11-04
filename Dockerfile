# crosstalk: a static binary in a small runtime image.
#
# The build stage compiles against musl so the runtime needs nothing beyond the base layer.
# The runtime runs as an unprivileged user whose uid/gid match the host's, because the config
# directory is bind-mounted and the daemon must be able to write the SQLite database, the CSV
# exports and any settings saved from the UI into it.

FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /src

# Dependencies first, so a source change does not re-download the world.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY data ./data
COPY config ./config
RUN cargo build --release --locked --bin crosstalk

FROM alpine:3.20 AS runtime

# Match the host user that owns the bind-mounted ./config. Override at build time:
#   docker build --build-arg PUID=$(id -u) --build-arg PGID=$(id -g) .
ARG PUID=1000
ARG PGID=1000

# ffmpeg (which includes ffprobe) is what `streams.probe` runs: IPTV Checker's stream check is
# two real subprocesses, not an HTTP request, and the plan puts them in the image so probing
# needs nothing on the host. Alpine installs them in /usr/bin, which is why
# `config/streams.example.toml` defaults `ffprobe_path` / `ffmpeg_path` there (IPTV Checker's
# own default, /usr/local/bin, is where its own Dockerfile put a static build).
# One layer, and the apk index is dropped in the same RUN so it costs no image space.
RUN apk add --no-cache ffmpeg \
    && addgroup -g "${PGID}" -S crosstalk \
    && adduser -u "${PUID}" -S -G crosstalk crosstalk \
    && mkdir -p /config \
    && chown "${PUID}:${PGID}" /config

COPY --from=builder /src/target/release/crosstalk /usr/local/bin/crosstalk

# The licence and the notices for the ported plugin travel with the image, as MIT requires.
COPY LICENSE THIRD_PARTY_NOTICES.md /usr/share/doc/crosstalk/

VOLUME ["/config"]
WORKDIR /config
USER crosstalk
EXPOSE 8730

# The daemon serves /health without authentication. The port here is the default
# `server.port` (8730): an install that overrides the port must override this too
# (docker run --health-cmd / compose `healthcheck:`), because a Dockerfile
# HEALTHCHECK cannot read the container's environment. BusyBox wget is always present.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8730/health || exit 1

ENTRYPOINT ["/usr/local/bin/crosstalk"]
CMD ["serve", "--config", "/config"]
