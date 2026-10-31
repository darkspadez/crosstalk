# crosstalk: a static binary in a minimal runtime image.
#
# The build stage compiles against musl so the runtime image needs no libc beyond what is
# already there, which keeps the final layer small and the binary portable.

FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev pkgconf
WORKDIR /src

# Dependencies first, so a source change does not re-download the world.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY data ./data
COPY config ./config
RUN cargo build --release --locked --bin crosstalk

FROM alpine:3.20 AS runtime
RUN addgroup -S crosstalk && adduser -S -G crosstalk crosstalk \
    && mkdir -p /config /data \
    && chown -R crosstalk:crosstalk /config /data
COPY --from=builder /src/target/release/crosstalk /usr/local/bin/crosstalk

# The configuration lives in a volume so it survives a container rebuild; the SQLite database
# and the CSV exports live under it for the same reason.
VOLUME ["/config"]
WORKDIR /config
USER crosstalk
EXPOSE 8730

# Loopback by default: the compose example binds 0.0.0.0 and sets a token.
ENTRYPOINT ["/usr/local/bin/crosstalk"]
CMD ["serve", "--config", "/config"]
