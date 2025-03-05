#!/bin/bash

load_env() {
	# Change to the root directory of the project where .env is located
	cd "$(dirname "$0")/.." || exit 1

	# Function to load .env file
	if [ -f ".env" ]; then
		set -a
		source ".env"
		set +a
	fi
}

check_postgres() {
	# Wait for PostgreSQL service to be ready
	until pg_isready -U "$POSTGRES_USER"; do
		echo "PostgreSQL is not ready yet. Retrying in 2 seconds..."
		sleep 2
	done

	echo "PostgreSQL is ready. Running migrations..."
}

update_db() {
	local cmd="midas-cli databento update --dataset futures --approval"

	# Execute the command and capture the exit status
	eval "$cmd"
	local status=$? # Capture the exit code of the last executed command

	if [ "$status" -ne 0 ]; then
		echo "Error: Failed to update database."
		exit 1
	else
		echo "Updating materialized view ..."
		refresh_continuous_calendar
		refresh_continuous_volume
		echo "Successfully updated database."
	fi
}

refresh_continuous_volume() {
	local cmd="psql -U $POSTGRES_USER -d market_data -c 'REFRESH MATERIALIZED VIEW futures_continuous_volume_windows'"

	eval "$cmd"
	local status=$?

	if [ "$status" -ne 0 ]; then
		echo "Error: Failed to refresh futures_continuous_volume_windows."
		exit 1
	else
		echo "Successfully refreshed futures_continuous_volume_windows."
	fi
}

refresh_continuous_calendar() {
	local cmd="psql -U $POSTGRES_USER -d market_data -c 'REFRESH MATERIALIZED VIEW futures_continuous_calendar_windows'"

	eval "$cmd"
	local status=$?

	if [ "$status" -ne 0 ]; then
		echo "Error: Failed to refresh futures_continuous_calendar_windows."
		exit 1
	else
		echo "Successfully refreshed futures_continuous_calendar_windows."
	fi
}

# Main
echo "Checking database available ..."
load_env
check_postgres

echo "Updating database ..."
update_db
