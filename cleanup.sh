#!/bin/bash
# USAGE: $0 [IMAGES_DOCKER_TAG [IMAGES_DOCKER_REPO]]
# EX: $0 1.0.1-dev registry.cloudbase.it
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/utils/common.sh"

TAG="$1"
REPO="$2"
if [[ ! -e "$CONFIG_BUILD_FILE" ]] && [[ ! "$TAG" ]] && [[ ! "$REPO" ]]; then
    echo "$CONFIG_BUILD_FILE not present, please rerun with:"
    echo "$0 <Docker tag> <Docker registry URL>"
    exit 1
fi

if [[ ! "$TAG" ]]; then
    TAG=`"$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n docker_images_tag`
fi
if [[ ! "$REPO" ]]; then
    REPO=`"$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n docker_registry`
    NS=`"$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n coriolis_containers_namespace`
    if [[ "$NS" ]]; then
        REPO="$REPO/$NS"
    fi
fi

remove_docker_container "coriolis-worker"
remove_docker_container "coriolis-conductor"
remove_docker_container "coriolis-api"
remove_docker_container "coriolis-web"
remove_docker_container "coriolis-web-proxy"
remove_docker_container "coriolis-replica-cron"
remove_docker_container "coriolis-logger"
remove_docker_container "influxdb"

remove_docker_image "$REPO/coriolis-worker:$TAG"
remove_docker_image "$REPO/coriolis-conductor:$TAG"
remove_docker_image "$REPO/coriolis-api:$TAG"
remove_docker_image "$REPO/coriolis-common:$TAG"
remove_docker_image "$REPO/coriolis-base:$TAG"
remove_docker_image "$REPO/coriolis-web:$TAG"
remove_docker_image "$REPO/coriolis-web-proxy:$TAG"
remove_docker_image "$REPO/coriolis-replica-cron:$TAG"
remove_docker_image "$REPO/coriolis-logger:$TAG"

docker volume rm influxdb 2>/dev/null|| true

bash "$BASE_DIR/licensing/cleanup.sh" "$TAG" "$REPO"
bash "$BASE_DIR/licensing-ui/cleanup.sh" "$TAG" "$REPO"
