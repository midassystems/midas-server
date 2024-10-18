#!/bin/bash

# # Wait until the files are available
# while [ ! -f /app/target/release/midas-cli ] || [ ! -f /app/target/release/libvendors.rlib ] || [ ! -f /app/target/release/libcli.rlib ]; do
# 	echo "Waiting for build artifacts..."
# 	sleep 5
# done

# Copy the compiled binary and libraries to /usr/local
cp /app/target/release/midas-cli /usr/local/bin/midas-cli
cp /app/target/release/libvendors.rlib /usr/local/lib/libvendors.rlib
cp /app/target/release/libcli.rlib /usr/local/lib/libcli.rlib

# Set the necessary permissions (if needed)
# chmod +x /usr/local/bin/midas-cli

echo "Files copied successfully."
