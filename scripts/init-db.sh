#!/bin/bash

set -e

check_database() {
	if PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -h "$POSTGRES_HOST" -d "postgres" -tAc "SELECT 1 FROM pg_database WHERE datname='$1'" | grep 1 &>/dev/null; then
		echo "$1 exists."
	else
		echo "Creating $1 database..."
		PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -h "$POSTGRES_HOST" -d "postgres" <<-EOSQL
			              CREATE DATABASE $1;
		EOSQL
	fi
}

# Wait for Postgres to be ready
until PGPASSWORD=$POSTGRES_PASSWORD psql -U "$POSTGRES_USER" -h "$POSTGRES_HOST" -d "postgres" -c '\q'; do
	echo "Postgres is unavailable - waiting..."
	sleep 1
done

echo "Postgres is up - executing command"

check_database "market_data"
check_database "trading_data"
