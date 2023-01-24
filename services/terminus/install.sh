#!/bin/bash

echo -n "Installing Salus service..."

# Ensure install directory exists
sudo mkdir /usr/local/tidepool/terminus

# Copy Python files
sudo cp services/terminus/Terminus.py /usr/local/tidepool/terminus &> /dev/null

# Copy systemd service files
sudo cp services/terminus/Terminus.service /etc/systemd/system/ &> /dev/null
sudo systemctl daemon-reload

# Ensure correct permissions
sudo chown -r ec2-user:ec2-user /usr/local/tidepool/terminus
echo "done."