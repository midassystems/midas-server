#!/bin/bash

check_postgres() {
	# Wait for PostgreSQL service to be ready
	until pg_isready -h "$POSTGRES_HOST" -U "$POSTGRES_USER"; do
		echo "PostgreSQL is not ready yet. Retrying in 2 seconds..."
		sleep 2
	done

	echo "PostgreSQL is ready. Running migrations..."
}

load_env() {
	# Change to the root directory of the project where .env is located
	cd "$(dirname "$0")/.." || exit 1

	# Function to load .env file
	if [ -z "$HISTORICAL_DATABASE_URL" ] && [ -z "$TRADING_DATABASE_URL" ] && [ -f ".env" ]; then
		set -a
		source ".env"
		set +a
	fi
}

refresh_continuous_volume() {
	local cmd="psql -U $POSTGRES_USER -d market_data -c 'REFRESH MATERIALIZED VIEW futures_continuous_volume_windows'"

	if [ "$cmd" -ne 0 ]; then
		echo "Error: Failed to refresh futures_continuous_volume_windows."
		exit 1
	else
		echo "Successfully refreshed futures_continuous_volume_windows."
	fi
}

refresh_continuous_calendar() {
	local cmd="psql -U $POSTGRES_USER -d market_data -c 'REFRESH MATERIALIZED VIEW futures_continuous_calendar_windows'"

	if [ "$cmd" -ne 0 ]; then
		echo "Error: Failed to refresh futures_continuous_calendar_windows."
		exit 1
	else
		echo "Successfully refreshed futures_continuous_calendar_windows."
	fi
}

main() {
	if [ "$#" -lt 1 ]; then
		echo "Usage: $0 <VIEW>"
		exit 1
	fi

	VIEW=$1

	check_postgres
	load_env

	case "$VIEW" in
	volume)
		if ! refresh_continuous_volume; then
			exit 1
		fi
		;;
	calendar)
		if ! refresh_continuous_calendar; then
			exit 1
		fi
		;;
	*)
		echo "Error: Invalid argument. Use 'volume' or 'calendar'." >&2
		exit 1
		;;
	esac
}
