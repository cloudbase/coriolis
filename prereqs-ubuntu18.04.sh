#!/usr/bin/env bash
#
# NOTE: This has only been tested on Ubuntu 18.04, though it should
# work relatively seamlessly on other releases as well.
#
set -o errexit
set -o xtrace
set -o pipefail

if [ $UID -ne 0 ]; then
    echo "Must be root!"
    exit 1
fi

CURRENT_DIR=$(dirname $0)

# Install Docker
apt-get install -y apt-transport-https ca-certificates curl \
                   gnupg-agent software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io jq
systemctl enable docker
systemctl start docker

# NOTE: IPv4 forwarding is required for rebuilding container images
# and resolving DNS inside the running containers:
sysctl -w net.ipv4.ip_forward=1
echo "net.ipv4.ip_forward=1" > /etc/sysctl.d/99-docker-ipv4-forwarding.conf

# NOTE: Required to generate SSL cert for Web proxy component
apt-get install -y openssl

# Install Python3 and various dev bits
apt-get install -y \
    git gcc make libffi-dev libssl-dev \
    python3 python3-pip

ln -sf python3 /usr/bin/python

# Required for our Ansible tasks
pip3 install -r $CURRENT_DIR/requirements.txt
