#!/bin/bash

export HISTORICAL_DATABASE_URL=postgres://postgres:password@localhost:5432/market_data
export INSTRUMENT_DATABASE_URL=postgres://postgres:password@localhost:5432/market_data
export TRADING_DATABASE_URL=postgres://postgres:password@localhost:5432/trading_data

historical() {
	cargo test -p historical -- --nocapture
}
trading() {
	cargo test -p trading -- --nocapture
}

instrument() {
	cargo test -p instrument -- --nocapture
}

all() {
	historical
	trading
	instrument
}

options() {
	echo "Which tests would you like to run?"
	echo "1 - Historical Service"
	echo "2 - Trading Service"
	echo "3 - Instrument Service"
	echo "4 - All"

}

# Main
while true; do
	options
	read -r option

	case $option in
	1)
		historical
		break
		;;
	2)
		trading
		break
		;;
	3)
		instrument
		break
		;;
	4)
		all
		break
		;;
	*) echo "Please choose a different one." ;;
	esac
done
