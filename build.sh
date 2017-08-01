#!/bin/bash
set -e

function clone_repo()
{
    repo_name=$1
    if [ ! -d $repo_name ]; then
        git clone git@bitbucket.org:cloudbase/$repo_name.git
    fi
}

VMWARE_VIX_DISK_LIB_URL=https://dl.dropboxusercontent.com/u/9060190/VMware-vix-disklib-6.5.0-5136645.x86_64.tar.gz

clone_repo coriolis-core
clone_repo coriolis-provider-oracle-vm
clone_repo coriolis-provider-vmware
clone_repo coriolis-provider-openstack

curl -s -o VMware-vix-disklib.tar.gz $VMWARE_VIX_DISK_LIB_URL

OS=${1:-ol7}

# For OL7, pull first the oraclelinux docker image from the Oracle registry
# (requires authentication):
# docker pull container-registry.oracle.com/os/oraclelinux:latest

docker build -t coriolis-base -f $OS/Dockerfile.base .
docker build -t coriolis-common -f $OS/Dockerfile.common .
docker build -t coriolis-api -f $OS/Dockerfile.api .
docker build -t coriolis-conductor -f $OS/Dockerfile.conductor .
docker build -t coriolis-worker -f $OS/Dockerfile.worker .
