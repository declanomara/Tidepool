# Tidepool
Tidepool is a project which aims to create a complete suite of tools for Forex trading. The project is currently in the early stages of development, and is not yet ready for use. This repository contains the services to run the Tidepool project, as well as the tidepool library which contains all the shared tools and utilities. For more information on the project, please visit the [Tidepool website](https://tidepool.finance).

The current services are:
- Mercury: Gathers data from the OANDA v20 API and passes it to Terminus
- Terminus: Receives data from Mercury and saves it to the database
- Salus: Monitors the health of all services, stopping and restarting them as necessary

## Mercury


## Terminus


## Salus

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