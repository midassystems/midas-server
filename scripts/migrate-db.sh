#!/bin/bash

# Change to the root directory of the project where .env is located
cd "$(dirname "$0")/.." || exit 1

# Function to load .env file
if [ -f ".env" ]; then
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
	echo "Trading_data database migrated."
elif [ "$DATABASE" == "historical" ]; then
	echo "HISTORICAL_DATABASE_URL: $HISTORICAL_DATABASE_URL"
	cd database/historical || exit 2
	DATABASE_URL=$HISTORICAL_DATABASE_URL sqlx migrate run
	echo "Market_data database migrated."
else
	echo "Unknown database: $DATABASE"
	exit 1
fi
