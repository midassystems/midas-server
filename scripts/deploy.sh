#!/bin/bash

# Function to display usage
usage() {
	echo "Usage: $0 {dev|prod}"
	exit 1
}

# Ensure that an argument is provided
if [ -z "$1" ]; then
	usage
fi

# Determine the environment
ENV=$1

# Run the host setup script for testing and production
if [[ "$ENV" == "prod" ]]; then
	# Ensure the setup script is executable
	chmod +x ./scripts/host-setup.sh

	echo "Running the host setup script..."

	# Check if the setup script ran successfully
	if [ "$(./scripts/host-setup.sh)" -ne 0 ]; then
		echo "Host setup script failed. Aborting deployment."
		exit 1
	fi
fi

# Proceed with Docker Compose deployment
echo "Running Docker Compose deployment for $ENV..."

if [[ "$ENV" == "dev" ]]; then
	mkdir -p data/processed_data
	mkdir -p logs
	mkdir -p postgres/data

	# Development deployment (most basic)
	if docker compose -f docker-compose.dev.yml up --build -d; then
		echo "Docker Compose deployment for development succeeded."
	else
		echo "Docker Compose deployment for development failed."
		exit 1
	fi
elif [[ "$ENV" == "prod" ]]; then
	# Production deployment
	# if docker compose --env-file .env --profile dev up --build -d; then

	if docker compose --env-file .env --profile prod up --build -d; then
		echo "Docker Compose deployment for production succeeded."
	else
		echo "Docker Compose deployment for production failed."
		exit 1
	fi

else
	usage # If an invalid option is provided, show the usage
fi
