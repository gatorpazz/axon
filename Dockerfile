# syntax=docker/dockerfile:1
FROM rust:1.88-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin axon && cp target/release/axon /axon

# Distroless, non-root: the image contains the binary and little else.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /axon /usr/local/bin/axon
# Binding a non-loopback address requires AXON_ACCESS_FILE; the server refuses
# to expose its development credentials.
ENV AXON_BIND=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/axon"]
