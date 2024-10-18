#!/bin/bash

# Variables
CONTAINER_NAME="server-postgres-1"     # Replace with your database container name
VOLUME_NAME="server_postgres_data"     # Replace with the volume you want to back up
BACKUP_DIR="/backups"                  # Local directory to store backups
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S") # Timestamp for the backup
BACKUP_FILE="$BACKUP_DIR/$VOLUME_NAME-backup-$TIMESTAMP.tar.gz"

# SSH details for your personal computer
REMOTE_USER="anthony" # Your local computer username
REMOTE_HOST="192.168.2.29"
REMOTE_BACKUP_DIR="/projects/midas/server_backups" # Directory on your personal computer for backups

# Step 1: Ensure the backup directory exists on the server
if [ ! -d "$BACKUP_DIR" ]; then
	mkdir -p "$BACKUP_DIR"
fi

# Step 2: Stop the container to ensure data consistency
docker stop "$CONTAINER_NAME"

# Step 3: Create the backup of the Docker volume
docker run --rm \
	-v "$VOLUME_NAME":/volume \
	-v "$BACKUP_DIR":/backup \
	alpine tar -czf "/backup/$VOLUME_NAME-backup-$TIMESTAMP.tar.gz" -C /volume ./

# Step 4: Restart the container after backup
docker start "$CONTAINER_NAME"

# Step 5: Transfer the backup to your personal computer using rsync over SSH
rsync -avz -e "ssh" "$BACKUP_FILE" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_BACKUP_DIR"

echo "Backup completed and transferred to your personal computer."
