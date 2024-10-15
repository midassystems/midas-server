#!/bin/bash

set -e

# Get the directory of this script (absolute path)
script_dir=$(cd "$(dirname "$0")" && pwd)

# Wait for Postgres to be ready
until PGPASSWORD="$POSTGRES_PASSWORD" psql -h "postgres" -U "$POSTGRES_USER" -d "postgres" -c '\q'; do
	echo "Postgres is unavailable - waiting..."
	sleep 1
done

echo "Postgres is up - executing command"

# Create databases or run any migrations
PGPASSWORD="$POSTGRES_PASSWORD" psql -h "postgres" -U "$POSTGRES_USER" -d "postgres" <<-EOSQL
	    CREATE DATABASE market_data;
	    CREATE DATABASE trading_data;
EOSQL
echo "Databases created successfully!"

"$script_dir/migrate-db.sh" market
"$script_dir/migrate-db.sh" trading
