# Use the official Rust image as a base
FROM rust:1.78 AS builder

# Set the working directory
WORKDIR /app

# Copy the Cargo.toml and Cargo.lock files
COPY Cargo.lock .
COPY Cargo.toml .

# Remove the `api` package from the workspace dynamically
RUN sed -i '/services\/historical/d' Cargo.toml
RUN sed -i '/services\/trading/d' Cargo.toml

# Copy the source code
COPY cli ./cli
COPY vendors ./vendors

# Pre-fetch the dependencies to leverage Docker's cache
RUN cargo fetch

# Build only the cli and vendors packages
RUN cargo build --release --package cli --package vendors

# Debug: Ensure the files are built and present
RUN ls -la /app/target/release

# Final stage
FROM debian:bookworm-slim

# Install cron and bash
RUN apt-get update && apt-get install -y cron bash && apt-get install -y jq && apt-get install vim -y

# # Copy the entire /app folder for debugging purposes
COPY --from=builder /app /app

# Scripts
RUN mkdir -p /app/scripts
COPY scripts/build-manager.sh /app/scripts/build-manager.sh

# Ensure script are executable
RUN chmod +x /app/scripts/build-manager.sh


