# crosstalk: a static binary in a small runtime image.
#
# The build stage compiles against musl so the runtime needs nothing beyond the base layer.
# The runtime runs as an unprivileged user whose uid/gid match the host's, because the config
# directory is bind-mounted and the daemon must be able to write the SQLite database, the CSV
# exports and any settings saved from the UI into it.

# Both bases are pinned by digest (Renovate's docker manager bumps tag and digest together).
# The builder tag names a Rust release so a rebuild is reproducible. rust-toolchain.toml
# (channel `stable`) is deliberately NOT copied in: rustup would honour it and download
# whatever stable is today, bypassing the pinned compiler. CI's toolchain follows `stable`
# and may run ahead of the image until Renovate bumps this tag.
FROM rust:1.99.1-alpine3.22@sha256:9a744dc47d1ac2a7d17fc3cedfc8b4928b1696dfcba3274ab25bfb40988a9ff0 AS builder
RUN apk add --no-cache musl-dev
WORKDIR /src

# Dependencies first, so a source change does not re-download the world.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY data ./data
COPY config ./config
RUN cargo build --release --locked --bin crosstalk

# Alpine 3.22 (supported to 2027-05): the ffmpeg below decodes untrusted provider streams, so
# the base must be a release that still gets security fixes.
FROM alpine:3.22@sha256:49340b346798875b5592e5d420241f2f9cfba5f47e62ebb1446a1b34734e898f AS runtime

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
# The package version is deliberately unpinned: Alpine's repositories drop superseded
# versions, so a pin would break the build on the next security update instead of taking it.
RUN apk add --no-cache ffmpeg \
    && addgroup -g "${PGID}" -S crosstalk \
    && adduser -u "${PUID}" -S -G crosstalk crosstalk \
    && mkdir -p /config \
    && chown "${PUID}:${PGID}" /config

COPY --from=builder /src/target/release/crosstalk /usr/local/bin/crosstalk

# The licence and the notices for the ported plugin travel with the image, as MIT requires.
# THIRD_PARTY_LICENSES (every Rust crate compiled in, from `cargo about generate`) is
# written into the build context by build.yml before the image build; the trailing `*`
# keeps a local `docker build` without it working.
COPY LICENSE THIRD_PARTY_NOTICES.md THIRD_PARTY_LICENSES* /usr/share/doc/crosstalk/

VOLUME ["/config"]
# The daemon writes only /config (database, exports, saved settings) and /tmp, so the
# container can run with a read-only root filesystem (see docker-compose.example.yml).
# The working directory is /tmp, not /config: anything a child process drops in its cwd
# (an ffprobe/ffmpeg side file) lands on the tmpfs, never in the persisted config. The
# CLI's `--config` default is relative, hence CROSSTALK_CONFIG_DIR for `docker exec`.
ENV CROSSTALK_CONFIG_DIR=/config
WORKDIR /tmp
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
