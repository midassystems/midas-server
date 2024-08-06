#!/bin/bash
set -e

# Use the default Postgres user and password from environment variables
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE market_data;
    CREATE DATABASE trading_data;
EOSQL

