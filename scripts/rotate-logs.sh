#!/bin/bash

# Variables
LOG_DIR="/app/logs"                    # Directory where logs are stored on the server
BACKUP_DIR="/log_backups"              # Directory on the server where log backups will be stored temporarily
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S") # Timestamp for the backup
ARCHIVE_FILE="$BACKUP_DIR/logs-$TIMESTAMP.tar.gz"

# SSH details for your personal computer
REMOTE_USER="anthony"                           # Your local computer username
REMOTE_HOST="192.168.2.29"                      # Your local computer's IP address
REMOTE_BACKUP_DIR="/projects/midas/log_backups" # Directory on your personal computer for backups

# Step 1: Ensure the backup directory exists on the server
if [ ! -d "$BACKUP_DIR" ]; then
	echo "Creating backup directory: $BACKUP_DIR"
	mkdir -p "$BACKUP_DIR"
fi

# Step 2: Archive the current log files
echo "Archiving logs from $LOG_DIR..."
tar -czf "$ARCHIVE_FILE" -C "$LOG_DIR" .

# Step 3: Transfer the archive to your personal computer using rsync over SSH
echo "Transferring logs to $REMOTE_USER@$REMOTE_HOST:$REMOTE_BACKUP_DIR..."
rsync -avz -e "ssh" "$ARCHIVE_FILE" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_BACKUP_DIR"

# Step 4: Clear the log files on the server (only after a successful transfer)
if [ $? -eq 0 ]; then
	echo "Logs transferred successfully. Clearing log files..."
	find "$LOG_DIR" -type f -exec truncate -s 0 {} \;
	echo "Log files cleared."
else
	echo "Error during transfer. Logs were not cleared."
fi

echo "Log rotation and transfer complete."
