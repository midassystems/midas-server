# Use the official Rust image as a base
FROM rust:1.78 as builder

# Set the working directory
WORKDIR /app

# Copy the Cargo.toml and Cargo.lock files
COPY Cargo.toml Cargo.lock ./

# Pre-fetch the dependencies to leverage Docker's cache
RUN cargo fetch

# Copy the source code
COPY src ./src

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
  && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the compiled binary from the builder
COPY --from=builder /app/target/release/midas-server /app/midas-server

# Set the command to run the binary
CMD ["./midas-server"]
