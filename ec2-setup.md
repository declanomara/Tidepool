# Tidepool EC2 Instance Setup Procedure:

## Instance launch and configuration:

The first step of setting up a Tidepool Finance data collection instance is to create the server following these steps.

### Configuring the server

1. Launch t3.medium instance, ensure interruption behavior is configured properly such that machine doesn't randomly terminate.
2. Attach EBS volume 'TidepoolDB' to new EC2 machine.
3. Use command `sudo lsblk -f` to check on file system on attached volumes.
4. If file system is not present follow [this](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-using-volumes.html) guide in order to either set up filesystem or detect existing file system.
5. Create directory for data, `sudo mkdir /data`
6. Mount data volume to data folder, `sudo mount /dev/sdf /data`

## Tidepool Software Setup

The second step of setting up a Tidepool Finance server is installing all software including MongoDB and proprietary software.

### Installing and configuring MongoDB
1. Follow [this](https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-amazon/) guide.
2. Verify you are running Amazon Linux using `grep ^NAME  /etc/*release`
3. Create a `/etc/yum.repos.d/mongodb-org-5.0.repo` file so that you can install MongoDB directly using `yum`
```
[mongodb-org-5.0]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/amazon/2/mongodb-org/5.0/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://www.mongodb.org/static/pgp/server-5.0.asc
```
4. Install the latest version of MongoDB using `sudo yum install -y mongodb-org`
5. Start MongoDB using `sudo systemctl start mongod`
6. Verify MongoDB is running using `sudo systemctl status mongod`
7. Using `mongosh` create admin user `tidepool`
8. Modify `/etc/mongod.conf` to enable security.
```
security:
  authorization: enabled
```
9. Restart MongoDB using `sudo systemctl restart mongod`




