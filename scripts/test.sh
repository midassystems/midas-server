#!/bin/bash

mkdir -p postgres/test_data
mkdir -p postgres/data
mkdir -p data/processed_data
mkdir -p logs

# Docker Compose command for setting up containers
setup_test_containers() {
	echo "Setting up containers for ..."
	docker compose -f docker-compose.test.yml up -d

	# Wait for services to initialize
	echo "Waiting for services to start..."
	sleep 10
}

teardown_test_containers() {
	echo "Tearing down containers ..."
	docker compose -f docker-compose.test.yml down
}

historical() {
	export HISTORICAL_DATABASE_URL=postgres://postgres:password@localhost:5433/market_data
	export TRADING_DATABASE_URL=postgres://postgres:password@localhost:5433/trading_data

	cargo test -p application historical -- --nocapture
}
trading() {
	export HISTORICAL_DATABASE_URL=postgres://postgres:password@localhost:5433/market_data
	export TRADING_DATABASE_URL=postgres://postgres:password@localhost:5433/trading_data
	cargo test -p application trading -- --nocapture
}

instrument() {
	export HISTORICAL_DATABASE_URL=postgres://postgres:password@localhost:5433/market_data
	export TRADING_DATABASE_URL=postgres://postgres:password@localhost:5433/trading_data

	cargo test -p application instrument -- --nocapture
}

all() {
	export HISTORICAL_DATABASE_URL=postgres://postgres:password@localhost:5433/market_data
	export TRADING_DATABASE_URL=postgres://postgres:password@localhost:5433/trading_data

	cargo test -p application -- --nocapture

}

system() {
	# Deloy the dev server
	echo "Setting up containers ..."

	export RAW_DIR=./data
	export PROCESSED_DIR=../data/processed_data

	docker compose -f docker-compose.dev.yml up --build -d

	# Wait for services to initialize
	echo "Waiting for services to start..."
	sleep 10

	cargo test -p tests -- --nocapture

	echo "Tearing down containers ..."

	docker compose -f docker-compose.dev.yml down
}

# Function to display usage
usage() {
	echo "Usage: $0 {historical|trading|instrument|all|system}"
	exit 1
}

# Ensure that an argument is provided
if [ -z "$1" ]; then
	usage
fi

# Determine the environment
ENV=$1

# Run the host setup script for testing and production
if [[ "$ENV" == "historical" ]]; then
	setup_test_containers
	if historical -ne 0; then
		teardown_test_containers
	fi
elif [[ "$ENV" == "trading" ]]; then
	setup_test_containers
	if trading -ne 0; then
		teardown_test_containers
	fi
elif [[ "$ENV" == "instrument" ]]; then
	setup_test_containers
	if instrument -ne 0; then
		teardown_test_containers
	fi
elif [[ "$ENV" == "all" ]]; then
	setup_test_containers
	if all -ne 0; then
		teardown_test_containers
	fi
elif [[ "$ENV" == "system" ]]; then
	system
else
	echo "Invalid option."
fi
