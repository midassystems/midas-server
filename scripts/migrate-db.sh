#!/bin/bash

echo "Waiting for PostgreSQL to be ready..."

# Wait for PostgreSQL service to be ready
until pg_isready -h "$POSTGRES_HOST" -U "$POSTGRES_USER"; do
	echo "PostgreSQL is not ready yet. Retrying in 2 seconds..."
	sleep 2
done

echo "PostgreSQL is ready. Running migrations..."

# Change to the root directory of the project where .env is located
cd "$(dirname "$0")/.." || exit 1

# Function to load .env file
if [ -z "$HISTORICAL_DATABASE_URL" ] && [ -z "$TRADING_DATABASE_URL" ] && [ -f ".env" ]; then
	set -a
	source ".env"
	set +a
fi

if [ "$#" -lt 1 ]; then
	echo "Usage: $0 <database>"
	exit 1
fi

DATABASE=$1

if [ "$DATABASE" == "trading" ]; then
	echo "TRADING_DATABASE_URL: $TRADING_DATABASE_URL"
	cd database/trading || exit 1
	DATABASE_URL=$TRADING_DATABASE_URL sqlx migrate run
	if [ $? -ne 0 ]; then
		echo "Error: Failed to migrate trading_data database."
		exit 1
	else
		echo "Trading_data database migrated."
	fi

elif [ "$DATABASE" == "historical" ]; then
	echo "HISTORICAL_DATABASE_URL: $HISTORICAL_DATABASE_URL"
	cd database/historical || exit 2
	DATABASE_URL=$HISTORICAL_DATABASE_URL sqlx migrate run
	if [ $? -ne 0 ]; then
		echo "Error: Failed to migrate market_data database."
		exit 1
	else
		echo "Market_data database migrated."
	fi
else
	echo "Unknown database: $DATABASE"
	exit 1
fi
