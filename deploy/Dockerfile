# ── Builder stage ─────────────────────────────────────────────────────────────
FROM rust:1.82-bookworm AS builder

WORKDIR /build
COPY . .

# Build release binaries and benchmark.
RUN cargo build --release --bin stream-manager --bin extent-node \
    && cargo build --release --bench pipeline_append_cluster

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM ubuntu:24.04

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
COPY --from=builder /build/target/release/pipeline_append_cluster-* /app/
RUN ln -sf /app/pipeline_append_cluster-* /app/pipeline_append_cluster || true

# Default entrypoint — overridden per container in Helm templates.
ENTRYPOINT ["/bin/bash"]
