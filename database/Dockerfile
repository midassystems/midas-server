# Use the official PostgreSQL image as a base
FROM postgres:14

# Install necessary packages for running Rust and SQLx CLI
RUN apt-get update && apt-get install -y --no-install-recommends \
  curl \
  build-essential \
  libssl-dev \
  libpq-dev \
  pkg-config \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

# Add cargo bin to PATH
ENV PATH="/root/.cargo/bin:${PATH}"

# Install sqlx-cli with the locked flag for version consistency
RUN /bin/bash -c "source $HOME/.cargo/env && cargo install sqlx-cli --locked"

# Set the working directory
WORKDIR /app

# Scripts
RUN mkdir -p /app/scripts
COPY scripts/init-db.sh /app/scripts/init-db.sh
COPY scripts/migrate-db.sh /app/scripts/migrate-db.sh

# Database
RUN mkdir -p /app/database
COPY database/market /app/database/market
COPY database/trading /app/database/trading

# Ensure scripts are executable
RUN chmod +x /app/scripts/init-db.sh
RUN chmod +x /app/scripts/migrate-db.sh


