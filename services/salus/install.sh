#!/bin/bash

echo -n "Installing Salus service..."

# Ensure install directory exists
sudo mkdir /usr/local/tidepool/salus

# Copy Python files
sudo cp services/salus/Salus.py /usr/local/tidepool/salus &> /dev/null

# Copy systemd service files
sudo cp services/salus/Salus.service /etc/systemd/system/ &> /dev/null
sudo systemctl daemon-reload

# Ensure correct permissions
sudo chown -r ec2-user:ec2-user /usr/local/tidepool/salus
echo "done."