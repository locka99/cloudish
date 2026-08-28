# ── Stage 1: dependency cache ─────────────────────────────────────────────────
# cargo-chef computes a "recipe" from Cargo.toml/Cargo.lock so that the heavy
# dependency compile layer is only re-run when those files change.

FROM rust:1.81-slim AS chef
RUN cargo install cargo-chef --locked
WORKDIR /build

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 2: build ────────────────────────────────────────────────────────────

FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json

# Build dependencies only (cached unless Cargo.toml / Cargo.lock change).
RUN cargo chef cook --release --recipe-path recipe.json

# Now copy the full source and build the binary.
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --bin cloudish

# ── Stage 3: runtime ──────────────────────────────────────────────────────────
# Use the official PostgreSQL image as the base so we get a working postgres
# installation without having to manage it ourselves.

FROM postgres:16-bookworm AS runtime

# Install runtime dependencies for the cloudish binary.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the compiled binary.
COPY --from=builder /build/target/release/cloudish /usr/local/bin/cloudish

# Copy the Docker-specific config and entrypoint.
COPY docker/cloudish.yaml /etc/cloudish/cloudish.yaml
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Persistent data volume for S3, DynamoDB, SQS, SES, Cognito, AppConfig.
VOLUME ["/data"]

# cloudish HTTP API
EXPOSE 4566

# cloudish RDS proxy (Postgres wire protocol)
EXPOSE 5433

# postgres is exposed on the standard port inside the container but is NOT
# published to the host by default — callers should use the RDS proxy on 5433.
EXPOSE 5432

# Postgres environment — the DB name must match rds.proxy_dsn in cloudish.yaml.
ENV POSTGRES_DB=cloudish \
    POSTGRES_USER=postgres \
    POSTGRES_PASSWORD=postgres \
    PGDATA=/var/lib/postgresql/data

ENTRYPOINT ["/entrypoint.sh"]
