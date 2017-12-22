#!/bin/bash

docker rm -f coriolis-worker
docker rm -f coriolis-conductor
docker rm -f coriolis-api
docker rm -f coriolis-web
docker rm -f coriolis-web-proxy
docker rm -f coriolis-replica-cron

TAG=1.0.1
REPO=registry.cloudbase.it

docker rmi $REPO/coriolis-worker:$TAG
docker rmi $REPO/coriolis-conductor:$TAG
docker rmi $REPO/coriolis-api:$TAG
docker rmi $REPO/coriolis-common:$TAG
docker rmi $REPO/coriolis-base:$TAG
docker rmi $REPO/coriolis-web:$TAG
docker rmi $REPO/coriolis-web-proxy:$TAG
docker rmi $REPO/coriolis-replica-cron:$TAG
