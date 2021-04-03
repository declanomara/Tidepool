#!/usr/bin/env bash

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

cd "$parent_path"

echo  "Checking for dependencies..."
pip3 install -r -u requirements.txt

echo "Killing old screens..."
screen -XS streaming kill
screen -XS dataprocessing kill

echo "Starting data processing listener..."
screen -dm -S 'dataprocessing' bash -c 'python3 process.py; exec bash'

echo "Starting data streaming script"
screen -dmS 'streaming' bash -c 'python3 stream.py; exec bash'

echo "Successfully started data collection systems"
