FROM rust:1.78 AS builder

# Install PostgreSQL client tools
RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*

RUN /bin/bash -c "cargo install sqlx-cli --locked"

# Set the working directory
WORKDIR /app

# Database
RUN mkdir -p /app/database
COPY database/historical /app/database/historical
COPY database/trading /app/database/trading

# Scripts
RUN mkdir -p /app/scripts

# Create databases if not exists
COPY scripts/init-db.sh /app/scripts/init-db.sh
RUN chmod +x /app/scripts/init-db.sh
# RUN /app/scripts/init-db.sh

# Migrate databases
COPY scripts/migrate-db.sh /app/scripts/migrate-db.sh
RUN chmod +x /app/scripts/migrate-db.sh
# RUN /bin/bash -c "/app/scripts/migrate-db.sh historical && /app/scripts/migrate-db.sh trading"

