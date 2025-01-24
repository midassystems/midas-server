#!/bin/bash

set -e

# Wait for Postgres to be ready
until PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -d "postgres" -c '\q'; do
	echo "Postgres is unavailable - waiting..."
	sleep 1
done

echo "Postgres is up - executing command"

# Create databases or run any migrations
PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -d "postgres" <<-EOSQL
	    CREATE DATABASE market_data;
	    CREATE DATABASE trading_data;
EOSQL
echo "Databases created successfully!"
