#!/bin/bash

# USAGE: $0 [IMAGES_DOCKER_TAG [IMAGES_DOCKER_TEMPLATE] [IMAGES_DOCKER_REPO]]
# EX: $0 1.0.1-dev ol7 registry.cloudbase.it

basedir=$(dirname "$(readlink -f "$0")")
get_config_value=$basedir/get_config_value.py

CONF_BUILD_FILE="$basedir/config-build.yml"
if test ! -e $CONF_BUILD_FILE; then
    if test ! "$1" && test ! "$2"; then
        echo "$CONF_BUILD_FILE not present, please rerun with:"
        echo "$0 <Docker tag> <Docker template (ol7/ubuntu18.04)> <Docker registry URL>"
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
TEMPLATE="$2"
if test ! "$TEMPLATE"; then
    TEMPLATE=`python $get_config_value -c $CONF_BUILD_FILE -n docker_template`
fi
REPO="$3"
if test ! "$REPO"; then
    REPO=`python $get_config_value -c $CONF_BUILD_FILE -n docker_registry`
    NS=`python $get_config_value -c $CONF_BUILD_FILE -n coriolis_containers_namespace`
    if [ "$NS" ]; then
        REPO="$REPO/$NS"
    fi
fi

docker rmi $REPO/$TEMPLATE/coriolis-worker:$TAG
docker rmi $REPO/$TEMPLATE/coriolis-conductor:$TAG
docker rmi $REPO/$TEMPLATE/coriolis-api:$TAG
docker rmi $REPO/$TEMPLATE/coriolis-common:$TAG
docker rmi $REPO/$TEMPLATE/coriolis-base:$TAG
docker rmi $REPO/$TEMPLATE/coriolis-web:$TAG
docker rmi $REPO/$TEMPLATE/coriolis-web-proxy:$TAG
docker rmi $REPO/$TEMPLATE/coriolis-replica-cron:$TAG
