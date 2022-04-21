# Kill any old screens
killall screen

cd ~
screen -dmS DataGatherer bash -c "cd Tidepool; python3 DataGatherer.py"
cd ~
screen -dmS DataMonitor bash -c "cd Tidepool; python3 DataMonitor.py"
