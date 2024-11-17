#!/bin/bash

historical() {
	export HISTORICAL_DATABASE_URL=postgres://postgres:password@localhost:5432/market_data
	cargo test -p historical -- --nocapture

}
trading() {
	export TRADING_DATABASE_URL=postgres://postgres:password@localhost:5432/trading_data
	cargo test -p trading -- --nocapture

}

all() {
	historical
	trading
}

options() {
	echo "Which tests would you like to run?"
	echo "1 - Historical Service"
	echo "2 - Trading Service"
	echo "3 - All"

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
		all
		break
		;;
	*) echo "Please choose a different one." ;;
	esac
done
