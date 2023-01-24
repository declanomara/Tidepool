#!/bin/bash

echo -n "Installing Mercury service..."

# Ensure install directory exists
sudo mkdir /usr/local/tidepool/mercury

# Copy Python files
sudo cp services/mercury/Mercury.py /usr/local/tidepool/mercury &> /dev/null
sudo cp services/mercury/HealthPublisher.py /usr/local/tidepool/mercury &> /dev/null

# Copy helper script
sudo cp services/mercury/start_mercury_workers.sh /usr/local/tidepool/mercury &> /dev/null

# Copy systemd service files
sudo cp services/mercury/Mercury.service /etc/systemd/system/ &> /dev/null
sudo cp services/mercury/Mercury@.service /etc/systemd/system/ &> /dev/null
sudo systemctl daemon-reload

# Ensure correct permissions
sudo chown -r ec2-user:ec2-user /usr/local/tidepool/mercury
echo "done."