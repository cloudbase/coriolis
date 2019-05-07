#!/bin/bash

# USAGE: $0 IMAGE_DOCKER_TAG IMAGE_DOCKER_REPO
# EX: $0 1.0.1-dev registry.cloudbase.it

if [ ! "$1" ] || [ ! "$2" ]; then
    echo "USAGE: $0 IMAGE_DOCKER_TAG IMAGE_DOCKER_REPO"
    exit 1
fi

docker rm -f coriolis-licensing-server
docker rmi $2/coriolis-licensing-server:$1
