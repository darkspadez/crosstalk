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

RUN addgroup -g "${PGID}" -S crosstalk \
    && adduser -u "${PUID}" -S -G crosstalk crosstalk \
    && mkdir -p /config \
    && chown "${PUID}:${PGID}" /config

COPY --from=builder /src/target/release/crosstalk /usr/local/bin/crosstalk

VOLUME ["/config"]
WORKDIR /config
USER crosstalk
EXPOSE 8730

ENTRYPOINT ["/usr/local/bin/crosstalk"]
CMD ["serve", "--config", "/config"]
