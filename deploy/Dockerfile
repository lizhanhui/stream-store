# ── Builder stage ─────────────────────────────────────────────────────────────
FROM rust:latest AS builder

WORKDIR /build
COPY . .

COPY deploy/config.toml $HOME/.cargo/config.toml

# Build release binaries and benchmark.
RUN cargo build --release --bin stream-manager --bin extent-node \
    && cargo build --release --bench pipeline_append_cluster

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binaries from builder.
COPY --from=builder /build/target/release/stream-manager /app/stream-manager
COPY --from=builder /build/target/release/extent-node /app/extent-node

# Benchmark binary lives under target/release/deps/ with a hash suffix;
# the build also produces a symlink-like name under target/release/.
# Copy all matching benchmark binaries.
COPY --from=builder /build/target/release/deps/pipeline_append_cluster-* /app/
RUN ln -sf /app/pipeline_append_cluster-* /app/pipeline_append_cluster || true

# Default entrypoint — overridden per container in Helm templates.
ENTRYPOINT ["/bin/bash"]
