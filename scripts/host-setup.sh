#!/bin/bash

# Define directories
LOG_DIR=/var/log/midas
DATA_DIR=/var/data/midas
SCRIPTS_DIR=/opt/midas/scripts

# Create directories if they don't exist
echo "Creating necessary directories for logs, data, and scripts..."

# Create log directory
if [ ! -d "$LOG_DIR" ]; then
	sudo mkdir -p "$LOG_DIR"
	sudo chown "$USER":"$USER" "$LOG_DIR"
	echo "Log directory created: $LOG_DIR"
else
	echo "Log directory already exists: $LOG_DIR"
fi

# Create data directory
if [ ! -d "$DATA_DIR" ]; then
	sudo mkdir -p "$DATA_DIR"
	sudo chown "$USER":"$USER" "$DATA_DIR"
	echo "Data directory created: $DATA_DIR"
else
	echo "Data directory already exists: $DATA_DIR"
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
sudo chmod 755 "$DATA_DIR"
sudo chmod 755 "$SCRIPTS_DIR"

# Optionally delete the repository directory (uncomment to enable)
# echo "Cleaning up repository directory..."
# sudo rm -rf /path/to/repo
# echo "Repository directory deleted."

echo "Host setup completed."
