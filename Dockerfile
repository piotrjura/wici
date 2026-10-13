# Relay server image. Test: scripts/test-docker.sh
FROM rust:1.99.0-slim-trixie AS build
# Use the image toolchain, not the extra components in rust-toolchain.toml.
ENV RUSTUP_TOOLCHAIN=1.99.0
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p wici-server && mkdir /data

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=build /src/target/release/wici-server /wici-server
# Named volumes copy this owner, so the non-root user can write.
COPY --from=build --chown=nonroot:nonroot /data /data
ENV WICI_LISTEN=0.0.0.0:8080 \
    WICI_DATABASE_URL=sqlite:/data/wici.db
VOLUME /data
EXPOSE 8080
USER nonroot
# The server stops cleanly on SIGINT. PID 1 ignores the default SIGTERM.
STOPSIGNAL SIGINT
ENTRYPOINT ["/wici-server"]
