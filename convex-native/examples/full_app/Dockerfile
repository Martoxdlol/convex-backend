# Multi-stage build for the full_app example.
#
# Build:
#   docker build -t convex_full_app:latest -f convex-native/examples/full_app/Dockerfile .
#
# Run (worker mode):
#   docker run --rm -p 4567:4567 \
#     -e CONVEX_MODE=worker \
#     -e CONVEX_WORKER_BIND_ADDR=0.0.0.0:4567 \
#     convex_full_app:latest

FROM rust:1.82-slim AS builder
WORKDIR /src
COPY . .
# Cache registry + target across rebuilds.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p convex_full_app_example && \
    cp target/release/convex_full_app /usr/local/bin/convex_full_app

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libssl3 \
 && rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --uid 10001 convex
USER convex

COPY --from=builder /usr/local/bin/convex_full_app /usr/local/bin/convex_full_app

EXPOSE 4567
ENV CONVEX_MODE=worker
ENV CONVEX_WORKER_BIND_ADDR=0.0.0.0:4567
ENV RUST_LOG=info,convex_native=debug

ENTRYPOINT ["/usr/local/bin/convex_full_app"]
