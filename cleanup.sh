#!/bin/bash

# USAGE: $0 [IMAGES_DOCKER_TAG [IMAGES_DOCKER_REPO]]
# EX: $0 1.0.1-dev registry.cloudbase.it

basedir=$(dirname "$(readlink -f "$0")")
get_config_value=$basedir/get_config_value.py

CONF_BUILD_FILE="$basedir/config-build.yml"
if test ! -e $CONF_BUILD_FILE; then
    if test ! "$1" && test ! "$2"; then
        echo "$CONF_BUILD_FILE not present, please rerun with:"
        echo "$0 <Docker tag> <Docker registry URL>"
        exit 1
    fi
fi

docker rm -f coriolis-worker
docker rm -f coriolis-conductor
docker rm -f coriolis-api
docker rm -f coriolis-web
docker rm -f coriolis-web-proxy
docker rm -f coriolis-replica-cron

TAG="$1"
if test ! "$TAG"; then
    TAG=`python $get_config_value -c $CONF_BUILD_FILE -n docker_images_tag`
fi
REPO="$2"
if test ! "$REPO"; then
    REPO=`python $get_config_value -c $CONF_BUILD_FILE -n docker_registry`
    NS=`python $get_config_value -c $CONF_BUILD_FILE -n coriolis_containers_namespace`
    if [ "$NS" ]; then
        REPO="$REPO/$NS"
    fi
fi

docker rmi $REPO/coriolis-worker:$TAG
docker rmi $REPO/coriolis-conductor:$TAG
docker rmi $REPO/coriolis-api:$TAG
docker rmi $REPO/coriolis-common:$TAG
docker rmi $REPO/coriolis-base:$TAG
docker rmi $REPO/coriolis-web:$TAG
docker rmi $REPO/coriolis-web-proxy:$TAG
docker rmi $REPO/coriolis-replica-cron:$TAG
