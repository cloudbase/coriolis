#!/usr/bin/env bash
set -e
set -x

DIR=$(dirname $0)

# NOTE: this has only been tested on Ubuntu 18.04, though it should
# work relatively seamlessly on other releases as well.

if [ $UID -ne 0 ]; then
    echo "Must be root!"
    exit 1
fi

# install Docker:
apt-get install -y docker.io
systemctl enable docker
systemctl start docker

# NOTE: IPv4 forwarding is required for rebuilding container images
# and resolving DNS inside the running containers:
sysctl -w net.ipv4.ip_forward=1
echo "net.ipv4.ip_forward=1" > /etc/sysctl.d/99-docker-ipv4-forwarding.conf

# NOTE: required to generate SSL cert for Web proxy component:
apt-get install -y openssl

# install python and various dev bits and bobs:
apt-get install -y \
    git gcc make libffi-dev libssl-dev \
    python3 python3-pip

# Required for our Ansible tasks
pip3 install -r $DIR/requirements.txt
