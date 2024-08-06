#!/bin/bash

# Function to load .env file
load_env() {
    if [ -f ".env" ]; then
        export $(grep -v '^#' .env | xargs)
    fi
}

# Load environment variables from .env file
load_env

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <database>"
    exit 1
fi

DATABASE=$1

if [ "$DATABASE" == "trading" ]; then
    echo "TRADING_DATABASE_URL: $TRADING_DATABASE_URL"
    cd trading
    DATABASE_URL=$TRADING_DATABASE_URL sqlx migrate run
    echo "Trading_data database migrated."
elif [ "$DATABASE" == "market" ]; then
    echo "MARKET_DATABASE_URL: $MARKET_DATABASE_URL"
    cd market
    DATABASE_URL=$MARKET_DATABASE_URL sqlx migrate run
    echo "Market_data database migrated."
else
    echo "Unknown database: $DATABASE"
    exit 1
fi

