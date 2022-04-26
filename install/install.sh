#!/usr/bin/env bash

sudo pip3 install -r requirements.txt

sudo cp install/services/*.service /lib/systemd/system/
sudo cp -r * /usr/share/Tidepool/


sudo systemctl daemon-reload
sudo systemctl start datagatherer.service
sudo systemctl start datamonitor.service