#!/bin/bash

cp /app/target/release/midas-cli /usr/local/bin/midas-cli
cp /app/target/release/libvendors.rlib /usr/local/lib/libvendors.rlib
cp /app/target/release/libcli.rlib /usr/local/lib/libcli.rlib

echo "Files copied successfully."
