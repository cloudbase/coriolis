#!/bin/bash

docker rm -f coriolis-worker
docker rm -f coriolis-conductor
docker rm -f coriolis-api

docker rmi coriolis-worker
docker rmi coriolis-conductor
docker rmi coriolis-api
docker rmi coriolis-common
docker rmi coriolis-base
