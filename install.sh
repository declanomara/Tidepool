#!/bin/bash

# Ensure script is running as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit
fi

# Ensure script is running in the correct directory
if [ ! -f "install.sh" ]; then
    echo "Please run from the root directory of the Tidepool release."
    exit
fi

# ------------------------------------------------------------

# Ensure install directory exists
echo -n "Ensuring install directory exists..."
if [ ! -d "/usr/local/tidepool" ]; then
    sudo mkdir /usr/local/tidepool
    sudo mkdir /usr/local/tidepool/logs
    sudo mkdir /usr/local/tidepool/configs
fi
sudo chown -r ec2-user:ec2-user /usr/local/tidepool

# Ensure install directory is empty
sudo rm -rf /usr/local/tidepool/*
echo "done."

# -------------------------------------------------------------

# Install Mercury
echo -n "Installing Mercury..."
sh services/mercury/install.sh
echo "done."

# Install Terminus
echo -n "Installing Terminus..."
sh services/terminus/install.sh
echo "done."

# Install Salus
echo -n "Installing Salus..."
sh services/salus/install.sh
echo "done."

# -------------------------------------------------------------

# Build and install compatible version of Python
if [ ! -f "/usr/local/bin/python3.11" ]; then
    echo "Installing Python 3.11.1..."
    sudo sh setup/install_python.sh
else
    echo "Python 3.11.1 already installed."
fi

# Create Tidepool virtual environment
echo -n "Creating Tidepool virtual environment..."
sudo mkdir /usr/local/tidepool/venv
sudo chown ec2-user:ec2-user /usr/local/tidepool/venv
/usr/local/bin/python3.11 -m venv /usr/local/tidepool/venv
echo "done."

# Install dependencies
echo -n "Installing dependencies..."
sudo /usr/local/tidepool/venv/bin/python3 -m pip install -r requirements.txt # &> /dev/null
sudo /usr/local/tidepool/venv/bin/python3 -m pip install tidepool/ # &> /dev/null
echo "done."

# --------------------------------------------------------------

# Ensure database is set up
echo -n "Ensuring database is set up..."
sudo sh setup/setup_db.sh
echo "done."

# --------------------------------------------------------------

# Copy config files.
if [ -d "config" ]; then
    echo -n "Copying config files..."
    sudo cp config/* /usr/local/tidepool/configs
    echo "done."
else
    echo "Please copy the config files to /usr/local/tidepool/configs."
fi

# Start services
echo -n "Starting services..."
sudo systemctl restart Mercury
sudo systemctl restart Terminus
sudo systemctl restart Salus
echo "done."
