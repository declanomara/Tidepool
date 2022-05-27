[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-%234ea94b.svg?style=for-the-badge&logo=mongodb&logoColor=white)


# Tidepool Core
This project is the core behind the Tidepool finance data collection. It contains two main componenets, the data gatherer  and the data monitor. 

## Data Gatherer

The data gatherer connects to the OANDA v20 API and collects price data for all currency pairs listed on the exchange. It attempts to collect every data point provided through the streaming API. In order to accomplish this it utilizes multiprocessing involving multiple processes for data collection, formatting, and saving. The data is saved in a MongoDB database running on and EC2 machine. This software utilizes autoscaling in order to keep up with varying load, primarily in saving data in the case of slow database connection or database restart, as well as occasional periods of rapid price movement resulting in a significant increase in data being collected and formatted.

## Data Monitor

The data monitor exists to check the health of the data gatherer. It does this implicitly by examining the database and recording changes to document numbers in the various currency collections. 

## Installation

Installation is mostly an automated process, the install process was designed to be done on an Amazon EC2 instance. 

### Requirements:

- systemd
- Python 3.8+
- Git
- pip requirements located in [requirements.txt](requirements.txt)

### Procedure:

1. Ensure requirements are met
2. Clone repo and run [install/install.sh](install/install.sh)
3. Ensure success using `systemctl status datagatherer` and `systemctl status datamonitor`

## Planned Updates
Located in [todo.md](todo.md)
