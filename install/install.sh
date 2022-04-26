#!/usr/bin/env bash

sudo pip3 install -r requirements.txt

sudo cp install/services/*.service /lib/systemd/system/
mkdir /usr/share/Tidepool
sudo cp -r * /usr/share/Tidepool/


sudo systemctl daemon-reload

sudo systemctl restart datagatherer.service
sudo systemctl restart datamonitor.service

sudo systemctl enable datagatherer.service
sudo systemctl enable datamonitor.service