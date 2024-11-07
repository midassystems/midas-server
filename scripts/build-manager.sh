#!/bin/bash

# Copy the compiled binary and libraries to /usr/local
# /var/midas b/c TrueNAS /usr/local is read-only
# cp /app/target/release/midas-cli /var/midas/bin/midas-cli
# cp /app/target/release/libvendors.rlib /var/midas/lib/libvendors.rlib
# cp /app/target/release/libcli.rlib /var/midas/lib/libcli.rlib

# On Traditional Linux server
cp /app/target/release/midas-cli /usr/local/bin/midas-cli
cp /app/target/release/libvendors.rlib /usr/local/lib/libvendors.rlib
cp /app/target/release/libcli.rlib /usr/local/lib/libcli.rlib

echo "Files copied successfully."
