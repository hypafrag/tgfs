# syntax=docker/dockerfile:1.6

# ---------- build stage ----------
FROM rust:1.94.1-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libfuse-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependencies: copy manifests and build a stub to populate the cargo
# registry/target cache before pulling in real sources.
COPY Cargo.toml Cargo.lock* ./
RUN mkdir -p src/bin/tgup && \
    echo 'fn main() {}' > src/main.rs && \
    echo 'fn main() {}' > src/bin/tgup/main.rs && \
    cargo build --release --bin tgfs && \
    rm -rf src target/release/deps/tgfs* target/release/tgfs*

COPY src ./src
RUN --mount=type=cache,target=/build/target,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    cargo build --release --bin tgfs && \
    cp target/release/tgfs /usr/local/bin/tgfs && \
    strip /usr/local/bin/tgfs

# ---------- runtime stage ----------
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        fuse \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /data

COPY --from=builder /usr/local/bin/tgfs /usr/local/bin/tgfs

ENTRYPOINT ["/usr/local/bin/tgfs"]
