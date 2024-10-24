#!/bin/bash

headers=("databento")
tickers="config/tickers.json"

update_last_update() {
	local header="$1"
	local current_date
	current_date=$(date '+%Y-%m-%d')

	# Update the last_update field for all active tickers in the header
	jq --arg header "$header" --arg date "$current_date" \
		'(.[$header][] | select(.active == true) | .last_update) = $date' "$tickers" >tmp.json && mv tmp.json "$tickers"
	echo "Updated $header active tickers 'last_update' field to $current_date."
}

update_tickers() {
	for header in "${headers[@]}"; do
		# Check if there are any active tickers to process
		echo "Processing $header..."

		# Use cli tool to update tickers historically
		# if dotenv -f .env midas-cli "$header" update; then
		if true; then
			# If CLI succeeds, update the last_update for all active tickers in the header
			update_last_update "$header"
		else
			echo "Failed to update tickers for $header using midas-cli. Exiting."
			exit 1
		fi
	done
}

# Main script
update_tickers
