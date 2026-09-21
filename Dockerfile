FROM rust:1-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
COPY static ./static
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/market-lab /usr/local/bin/market-lab
COPY static ./static
RUN mkdir -p /app/data
ENV PORT=8080
ENV DATA_DIR=/app/data
EXPOSE 8080
VOLUME ["/app/data"]
CMD ["market-lab"]
