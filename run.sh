#!/usr/bin/env bash

echo "Killing old screens..."
screen -XS streaming kill
screen -XS dataprocessing kill

echo "Starting data processing listener..."
screen -dm -S 'dataprocessing' bash -c 'python3 processing.py; exec bash'

echo "Starting data streaming script"
screen -dmS 'streaming' bash -c 'python3 streaming.py; exec bash'

echo "Successfully started data collection"
