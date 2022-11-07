#!/usr/bin/env bash

# Ensure running as root
if [ "$EUID" -ne 0 ]
    then echo "Install script must be run as root."
    exit
fi

INSTALL_DIR="/usr/share/Tidepool"

# Check if in virtual environment already
if [[ "$VIRTUAL_ENV" != "" ]]
then
  # In virtual environment
  echo "Already running in virtual environment."

else
  # Not in virtual environment

  # Check if in virtual environment exists
  if [ -d "./venv" ]
  then
    echo "Virtual environment already exists."
  else
    # Create virtual environment
    echo "Creating virtual environment..."
    python3.10 -m venv ./venv
    echo "Virtual environment created."
  fi

  # Activate virtual environment
  echo "Activating virtual environment..."
  source "./venv/bin/activate"
  echo "Virtual environment activated."
fi

# Install python requirements
echo "Installing python requirements..."
python3 -m pip install -r requirements.txt
echo "Done."

# Move services into systemd directory
echo "Installing systemd services..."
sudo cp install/services/*.service /lib/systemd/system/
echo "Done."

# Move program files into correct directory
if [ -d $INSTALL_DIR ]
then
  echo "Install directory already exists."
else
  echo "Creating install directory: $INSTALL_DIR"
  mkdir $INSTALL_DIR
  echo "Created directory $INSTALL_DIR."
fi

echo "Moving required files to $INSTALL_DIR"
sudo cp -r ./* $INSTALL_DIR

# Reload and start new services

echo "Reloading systemd..."
systemctl daemon-reload
echo "Done."

echo "Starting Tidepool services..."
systemctl restart datagatherer.service
systemctl restart datamonitor.service
echo "Done."

echo "Enabling Tidepool services..."
systemctl enable datagatherer.service
systemctl enable datamonitor.service
echo "Done."