#!/bin/bash

# This script ensures that 
# 1. The database volume is mounted at /persistent
# 2. MongoDB is installed
# 3. The database is initialized
# 4. The database is running

DB_DRIVE="/dev/nvme1n1"
DB_MOUNT_POINT="/persistent"
CONFIG_DIRECTORY="/usr/local/tidepool/configs"





# Ensure database volume is mounted at /persistent

# Check if the database drive exists
if [ -b $DB_DRIVE ];
then
    echo "Database drive exists."
else
    echo "Database drive does not exist."
    exit
fi

# Check if DB_MOUNT_POINT exists
if [ -d $DB_MOUNT_POINT ]; 
then
    echo $DB_MOUNT_POINT "already exists."
else
    echo "Creating database mount point at" $DB_MOUNT_POINT
    mkdir $DB_MOUNT_POINT
    echo "Successfully created database mount point at" $DB_MOUNT_POINT
fi

# Check if $DB_MOUNT_POINT/mongo exists
if [ -d "$DB_MOUNT_POINT/mongo" ]; 
then
    echo "MongoDB drive already mounted."
else
    echo "Mounting MongoDB drive at" $DB_MOUNT_POINT
    mount $DB_DRIVE $DB_MOUNT_POINT
    echo "Successfully mounted MongoDB drive at" $DB_MOUNT_POINT
fi


# Ensure MongoDB is installed

# Ensure MongoDB repo is available
cp "$CONFIG_DIRECTORY/mongodb-org-6.0.repo" "/etc/yum.repos.d/"

# Check if MongoDB is installed
if [ -d "/usr/bin/mongod" ]; 
then
    echo "MongoDB already installed."
else
    echo "Installing MongoDB..."
    yum install -y mongodb-org
    echo "Successfully installed MongoDB."
fi

# Check if configured
if [ ! -f "/etc/mongod.conf" ];
then
    echo "MongoDB config file already exists."
else
    echo "Creating MongoDB config file..."
    cp "$CONFIG_DIRECTORY/mongod.conf" "/etc/"
    echo "Successfully created MongoDB config file."
fi

# Ensure mongod:mongod owns $DB_MOUNT_POINT/mongo
chown -R mongod:mongod $DB_MOUNT_POINT/mongo

# Start MongoDB
echo "Starting MongoDB..."
systemctl enable mongod.service
systemctl daemon-reload
systemctl restart mongod.service
echo "Started MongoDB successfully."
