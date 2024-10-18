#!/bin/bash

# Test CLI
dotenv -f .env.dev cargo test -p cli -- --nocapture

# Test Vendors
cargo test -p vendors -- --nocapture

# Test services
export HISTORICAL_DATABASE_URL=postgres://postgres:password@localhost:5432/market_data
export TRADING_DATABASE_URL=postgres://postgres:password@localhost:5432/trading_data

cargo test -p historical -- --nocapture
cargo test -p trading -- --nocapture
