#!/bin/bash

ENV=$1

headers=("databento")
if [ "$ENV" == "dev" ]; then
	tickers="config/tickers.json"
else
	tickers="$HOME/.config/midas/tickers.json"
fi

update_last_update() {
	local header="$1"
	local current_date
	current_date=$(date '+%Y-%m-%d')

	# Update the last_update field for all active tickers in the header
	jq --arg header "$header" --arg date "$current_date" \
		'(.[$header][] | select(.active == true) | .last_update) = $date' "$tickers" >/tmp/tmp.json && mv /tmp/tmp.json "$tickers"
	echo "Updated $header active tickers 'last_update' field to $current_date."
}

update_tickers() {
	for header in "${headers[@]}"; do
		# Check if there are any active tickers to process
		echo "Processing $header..."

		# Use cli tool to update tickers dynamically based on environment
		if [ "$ENV" == "dev" ]; then
			# Call midas-cli with development-specific arguments
			if dotenv -f .env midas-cli "$header" update; then
				update_last_update "$header"
			else
				echo "Failed to update tickers for $header using midas-cli. Exiting."
				exit 1
			fi
		else
			# Call midas-cli with production-specific arguments
			if midas-cli "$header" update; then
				update_last_update "$header"
			else
				echo "Failed to update tickers for $header using midas-cli. Exiting."
				exit 1
			fi
		fi
	done
}

# Main script
update_tickers
