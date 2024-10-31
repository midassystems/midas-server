#!/bin/bash

cli() {
	dotenv -f .env.dev cargo test -p cli -- --nocapture
}

vendors() {
	cargo test -p vendors -- --nocapture
}

historical() {
	# Test services
	export HISTORICAL_DATABASE_URL=postgres://postgres:password@localhost:5432/market_data
	cargo test -p historical -- --nocapture

}
trading() {
	export TRADING_DATABASE_URL=postgres://postgres:password@localhost:5432/trading_data
	cargo test -p trading -- --nocapture

}

all() {
	cli
	vendors
	historical
	trading
}

options() {
	echo "Which tests would you like to run?"
	echo "1 - CLI"
	echo "2 - Vendors"
	echo "3 - Historical Service"
	echo "4 - Trading Service"
	echo "5 - All"

}

# Main
while true; do
	options
	read -r option

	case $option in
	1)
		cli
		break
		;;
	2)
		vendors
		break
		;;
	3)
		historical
		break
		;;
	4)
		trading
		break
		;;
	5)
		all
		break
		;;
	*) echo "Please choose a different one." ;;
	esac
done
