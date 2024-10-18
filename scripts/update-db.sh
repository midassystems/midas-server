#!/bin/bash

type=$1 # 'update',' bulkupdate', 'add'
start=$2
end=$3
headers=("databento")
tickers="config/tickers.json"

# Main script
if [ "$type" = "update" ]; then
	update_tickers
elif [ "$type" = "bulk-update" ]; then
	add_tickers "bulk-update" "config/update_tickers.json" "$start" "$end"
elif [ "$type" = "add" ]; then
	add_tickers "add" "config/add_tickers.json" "$start" "$end"
else
	echo "Invalid option."

fi

# Helper functions start here
add_tickers() {
	# Accept start and end dates as arguments
	command=$1
	update_file=$2
	start_date=$3
	end_date=$4
	# tickers="config/tickers.json"

	# Check if both start and end dates are provided
	if [ -z "$start_date" ] || [ -z "$end_date" ]; then
		echo "Usage: $0 <start_date> <end_date>"
		exit 1
	fi

	for header in "${headers[@]}"; do
		# Check if the header has content to process in bulk_jobs
		if jq -e ".${header} | length > 0" "$update_file" >/dev/null; then
			echo "Processing $header..."

			# Use cli tool to add tickers historically
			if ! ./midas-cli "$header" "$command" --start "$start_date" --end "$end_date"; then
				echo "Failed to $command tickers for $header using midas-cli. Exiting."
				exit 1
			fi

			# Append and check if successful
			if jq -s ".[0].${header} += .[1].${header} | unique_by(.ticker).[0]" $tickers "$update_file" >temp.json && mv temp.json $tickers; then
				# cat temp.json
				echo "$header tickers appended to tickers.json successfully."

				# Reset the processed header's data in new_tickers
				jq ".${header} = []" "$update_file" >temp.json && mv temp.json "$update_file"
				echo "$header tickers removed from $update_file."
			else
				echo "Failed to append data for $header."
			fi
		else
			echo "No new tickers to process for $header."
		fi
	done
}

update_tickers() {
	# command=$1
	# tickers="config/tickers.json"

	for header in "${headers[@]}"; do
		# Check if the header has content to process in bulk_jobs
		if jq -e ".${header} | length > 0" "$tickers" >/dev/null; then
			echo "Processing $header..."

			# Use cli tool to add tickers historically
			if ! ./midas-cli "$header" update; then
				echo "Failed to update tickers for $header using midas-cli. Exiting."
				exit 1
			fi
		else
			echo "No tickers to process for $header."
		fi
	done
}
