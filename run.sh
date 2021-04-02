#!/usr/bin/env bash

echo "Killing old screens..."
screen -XS dataprocessing kill
screen -XS streaming kill

echo "Starting data processing listener..."
screen -dmS 'dataprocessing' bash -c 'python3 processing.py'

echo "Starting data streaming script"
screen -dmS 'streaming' bash -c 'python3 streaming.py'

echo "Successfully started data collection"
