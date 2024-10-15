# Use the official Rust image as a base
FROM rust:1.78 as builder

# Set the working directory
WORKDIR /app


# Copy the Cargo.toml and Cargo.lock files
COPY Cargo.lock .
COPY Cargo.toml .

# Remove the `api` package from the workspace dynamically
RUN sed -i '/api/d' Cargo.toml

# Copy the source code
COPY cli ./cli
COPY vendors ./vendors

# Pre-fetch the dependencies to leverage Docker's cache
RUN cargo fetch

# Build only the cli and vendors packages
RUN cargo build --release --package cli --package vendors

# Final stage
FROM debian:bookworm-slim

# Install cron and bash
RUN apt-get update && apt-get install -y cron bash && apt-get install -y jq && apt-get install vim -y

# Copy the entire /app folder for debugging purposes
COPY --from=builder /app /app

# # Copy the compiled binary and library from the builder stage to the /output directory in the container
COPY --from=builder /app/target/release/libvendors.rlib /app/libvendors.rlib
COPY --from=builder /app/target/release/libcli.rlib /app/libcli.rlib
COPY --from=builder /app/target/release/midas-cli /app/midas-cli


# Copy cron jobs and scripts
COPY scripts/cronjobs /etc/cron.d/cronjobs
COPY scripts/. /app/scripts


# Give execution rights on the cron job and ensure the binary is executable
RUN chmod 0644 /etc/cron.d/cronjobs && \
    chmod +x /app/cli && \
    crontab /etc/cron.d/cronjobs

# Start the cron service in the foreground
CMD ["cron", "-f"]

