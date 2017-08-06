#!/bin/bash
set -e

CORIOLIS_CONF_PATH="$PWD/etc"
LOGS_PATH="$PWD/logs"
MYSQL_DATA_PATH="$PWD/mysql"
RABBITMQ_DATA_PATH="$PWD/rabbitmq"

MYSQL_ROOT_PASSWORD=Passw0rd
# NOTE: container-registry.oracle.com requires authentication
MYSQL_DOCKER_IMAGE=container-registry.oracle.com/mysql/community-server:latest

RABBITMQ_USER=coriolis
RABBITMQ_PASSWORD=Passw0rd
RABBITMQ_ERLANG_COOKIE=Passw0rd

NETWORK_NAME=coriolis_nw

mkdir -p $MYSQL_DATA_PATH
mkdir -p $LOGS_PATH
mkdir -p $RABBITMQ_DATA_PATH

docker network create --driver bridge $NETWORK_NAME || true

echo "Starting RabbitMQ"
docker run --network=$NETWORK_NAME --hostname=coriolis-rabbitmq -p 5672:5672 -itd \
--name coriolis-rabbitmq -v $RABBITMQ_DATA_PATH:/var/lib/rabbitmq  \
-e RABBITMQ_DEFAULT_USER=$RABBITMQ_USER -e RABBITMQ_DEFAULT_PASS=$RABBITMQ_PASSWORD \
-e RABBITMQ_ERLANG_COOKIE=$RABBITMQ_ERLANG_COOKIE --restart unless-stopped rabbitmq:3

echo "Starting MySQL"
docker run --network=$NETWORK_NAME --hostname=coriolis-mysql \
--name coriolis-mysql -v $MYSQL_DATA_PATH:/var/lib/mysql \
-e MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD -itd --restart unless-stopped $MYSQL_DOCKER_IMAGE

echo "Starting API"
docker run --network=$NETWORK_NAME -p:7667:7667 -v $CORIOLIS_CONF_PATH:/etc/coriolis \
-v $LOGS_PATH:/var/log/coriolis -itd --restart unless-stopped --name coriolis-api coriolis-api:latest

echo "Starting conductor"
docker run --network=$NETWORK_NAME -v $CORIOLIS_CONF_PATH:/etc/coriolis \
-v $LOGS_PATH:/var/log/coriolis -itd --restart unless-stopped --name coriolis-conductor coriolis-conductor:latest

echo "Starting worker"
docker run --network=$NETWORK_NAME -v $CORIOLIS_CONF_PATH:/etc/coriolis \
-v $LOGS_PATH:/var/log/coriolis -itd --restart unless-stopped --name coriolis-worker coriolis-worker:latest
