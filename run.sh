#!/bin/bash
set -e

CORIOLIS_CONF_PATH="$PWD/etc"
LOGS_PATH="$PWD/logs"

mkdir -p $LOGS_PATH

echo "Starting API"
docker run -p:7667:7667 -v $CORIOLIS_CONF_PATH:/etc/coriolis -v $LOGS_PATH:/var/log/coriolis -d --restart unless-stopped --name coriolis-api coriolis-api:latest
echo "Starting conductor"
docker run -v $CORIOLIS_CONF_PATH:/etc/coriolis -v $LOGS_PATH:/var/log/coriolis -d --restart unless-stopped --name coriolis-conductor coriolis-conductor:latest
echo "Starting worker"
docker run -v $CORIOLIS_CONF_PATH:/etc/coriolis -v $LOGS_PATH:/var/log/coriolis -d --restart unless-stopped --name coriolis-worker coriolis-worker:latest
