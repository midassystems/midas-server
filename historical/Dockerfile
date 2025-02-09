# Use the official Rust image as a base
FROM rust:1.78 AS builder

# Set the working directory
WORKDIR /app

# Copy the Cargo.toml and Cargo.lock files
COPY historical/Cargo.toml .
COPY Cargo.lock .

# Copy the source code
COPY historical/src ./src

# Create data dir(where data files are dumped)
RUN mkdir -p /app/bin

# Pre-fetch the dependencies to leverage Docker's cache
RUN cargo fetch

# Install OpenSSL for database connections and other dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
  libssl-dev \
  pkg-config \
  && rm -rf /var/lib/apt/lists/*

# Build the application
RUN cargo build --release

# Use a more recent Debian image for deployment
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
  libssl3 \
  ca-certificates \
  postgresql-client \
  && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the compiled binary from the builder
COPY --from=builder /app/target/release/libhistorical.rlib /app/libhistorical.rlib
COPY --from=builder /app/target/release/midas-historical /app/midas-historical

