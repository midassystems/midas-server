#!/bin/bash

# Define directories
LOG_DIR=/opt/midas/logs
DATA_DIR=/opt/midas/data
RAW_DIR=/opt/midas/data/raw_data/
PROCESSED_DIR=/opt/midas/data/processed_data/
SCRIPTS_DIR=/opt/midas/scripts
POSTGRES_DIR=/opt/midas/postgres
POSTGRES_DATA=/opt/midas/postgres/data
CONFIG_DIR=~/.config/midas
BIN_DIR=/usr/local/bin
LIB_DIR=/usr/local/lib

# Create directories if they don't exist
echo "Creating necessary directories for logs, data, and scripts..."

# Create config directory
if [ ! -d "$HOME/.config" ]; then
	sudo mkdir -p "$HOME/.config"
fi

if [ ! -d "$CONFIG_DIR" ]; then
	sudo mkdir -p "$CONFIG_DIR"
	sudo chown "$USER":"$USER" "$CONFIG_DIR"
	# cp tickers_template.json "$CONFIG_DIR/tickers.json"
	echo "Config directory set up."
else
	echo "Config directory already exists: $CONFIG_DIR"
fi

# Create log directory
if [ ! -d "$LOG_DIR" ]; then
	sudo mkdir -p "$LOG_DIR"
	sudo chown "$USER":"$USER" "$LOG_DIR"
	echo "Log directory created: $LOG_DIR"
else
	echo "Log directory already exists: $LOG_DIR"
fi

touch $LOG_DIR/historical.log
touch $LOG_DIR/trading.log

# Create postgres docker volume directory
if [ ! -d "$POSTGRES_DIR" ]; then
	sudo mkdir -p "$POSTGRES_DIR"
	sudo chown "$USER":"$USER" "$POSTGRES_DIR"
	echo "Postgres directory created: $POSTGRES_DIR"
else
	echo "Postgres directory already exists: $POSTGRES_DIR"
fi

if [ ! -d "$POSTGRES_DATA" ]; then
	sudo mkdir -p "$POSTGRES_DATA"
	sudo chown "$USER":"$USER" "$POSTGRES_DATA"
	echo "Postgres data directory created: $POSTGRES_DATA"
else
	echo "Postgres data directory already exists: $POSTGRES_DATA"
fi

# Create data directory
if [ ! -d "$DATA_DIR" ]; then
	sudo mkdir -p "$DATA_DIR"
	sudo chown "$USER":"$USER" "$DATA_DIR"
	echo "Data directory created: $DATA_DIR"
else
	echo "Data directory already exists: $DATA_DIR"
fi

# Create data directory
if [ ! -d "$PROCESSED_DIR" ]; then
	sudo mkdir -p "$PROCESSED_DIR"
	sudo chown "$USER":"$USER" "$PROCESSED_DIR"
	echo "Data directory created: $PROCESSED_DIR"
else
	echo "Data directory already exists: $PROCESSED_DIR"
fi

# Create data directory
if [ ! -d "$RAW_DIR" ]; then
	sudo mkdir -p "$RAW_DIR"
	sudo chown "$USER":"$USER" "$RAW_DIR"
	echo "Data directory created: $RAW_DIR"
else
	echo "Data directory already exists: $RAW_DIR"
fi

# Create scripts directory
if [ ! -d "$BIN_DIR" ]; then
	sudo mkdir -p "$BIN_DIR"
	sudo chown "$USER":"$USER" "$BIN_DIR"
	echo "Binary directory created: $BIN_DIR"
else
	echo "Binary directory already exists: $BIN_DIR"
fi

# Create scripts directory
if [ ! -d "$LIB_DIR" ]; then
	sudo mkdir -p "$LIB_DIR"
	sudo chown "$USER":"$USER" "$LIB_DIR"
	echo "Library directory created: $LIB_DIR"
else
	echo "Library directory already exists: $LIB_DIR"
fi

# Create scripts directory
if [ ! -d "$SCRIPTS_DIR" ]; then
	sudo mkdir -p "$SCRIPTS_DIR"
	sudo chown "$USER":"$USER" "$SCRIPTS_DIR"
	echo "Scripts directory created: $SCRIPTS_DIR"
else
	echo "Scripts directory already exists: $SCRIPTS_DIR"
fi

# Copy scripts to the scripts directory
echo "Copying scripts to $SCRIPTS_DIR..."
sudo cp -r ./scripts/* "$SCRIPTS_DIR"
sudo chown -R "$USER":"$USER" "$SCRIPTS_DIR"
echo "Scripts copied to $SCRIPTS_DIR."

# Set permissions (ensure Docker containers can write to these directories)
sudo chmod 755 "$LOG_DIR"
sudo chmod 755 "$POSTGRES_DIR"
sudo chmod 755 "$POSTGRES_DATA"
sudo chmod 755 "$DATA_DIR"
sudo chmod 755 "$RAW_DIR"
sudo chmod 755 "$PROCESSED_DIR"
sudo chmod 755 "$SCRIPTS_DIR"
sudo chmod 755 "$CONFIG_DIR"
sudo chmod 755 "$BIN_DIR"
sudo chmod 755 "$LIB_DIR"

echo "Host setup completed."
