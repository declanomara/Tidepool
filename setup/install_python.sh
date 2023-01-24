#!/bin/bash
cd /tmp/

# Install dependencies (development tools)
sudo yum update -y
sudo yum groupinstall "Development Tools" -y
sudo yum erase openssl-devel -y
sudo yum install openssl11 openssl11-devel  libffi-devel bzip2-devel wget -y

# Download Python 3.11.1
wget https://www.python.org/ftp/python/3.11.1/Python-3.11.1.tgz
tar -xvf Python-3.11.1.tgz
cd Python-3.11.1

# Install Python 3.11.1
./configure --enable-optimizations
make -j $(nproc)
sudo make altinstall

# Ensure Python 3.11.1 is installed
python3.11 --version

# Clean up
cd ..
sudo rm -rf Python-3.11.1
rm Python-3.11.1.tgz
