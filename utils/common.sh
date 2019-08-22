#!/bin/bash

UTILS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GET_CONFIG_VALUE_SCRIPT="$UTILS_DIR/get_config_value.py"
SET_CONFIG_VALUE_SCRIPT="$UTILS_DIR/set_config_value.py"
CONFIG_FILE=$(readlink -f "$UTILS_DIR/../config.yml")
CONFIG_BUILD_FILE=$(readlink -f "$UTILS_DIR/../config-build.yml")


remove_docker_container() {
    local CONTAINER_NAME="$1"
    if [[ "$CONTAINER_NAME" = "" ]]; then
        return
    fi
    if [[ "$(docker ps -q -f name="$CONTAINER_NAME")" != "" ]]; then
        docker rm -f "$CONTAINER_NAME"
    fi
}

remove_docker_image() {
    local DOCKER_IMAGE="$1"
    if [[ "$DOCKER_IMAGE" = "" ]]; then
        return
    fi
    if [[ "$(docker images -q "$DOCKER_IMAGE")" != "" ]]; then
        docker rmi -f "$DOCKER_IMAGE"
    fi
}

remove_docker_volume() {
    local DOCKER_VOLUME="$1"
    if [[ "$DOCKER_VOLUME" = "" ]]; then
        return
    fi
    if [[ "$(docker volume ls -q -f name="$DOCKER_VOLUME")" != "" ]]; then
        docker volume rm "$DOCKER_VOLUME"
    fi
}

new_config_file() {
    local CONF_FILE_PATH="$1"
    if [[ -f "$CONF_FILE_PATH" ]]; then
        return
    fi
    if [[ ! -f "${CONF_FILE_PATH}.sample" ]]; then
        echo "Sample config file does not exist"
        echo "Cannot create new config file: $CONF_FILE_PATH"
        return 1
    fi
    cp "${CONF_FILE_PATH}.sample" "$CONF_FILE_PATH"
}

set_config_file_random_value() {
    local CONF_FILE_PATH="$1"
    local NAME="$2"
    if [[ -z $(python "$GET_CONFIG_VALUE_SCRIPT" -c "$CONF_FILE_PATH" -n "$NAME") ]]; then
        python "$SET_CONFIG_VALUE_SCRIPT" -c "$CONF_FILE_PATH" -n "$NAME" -v "$(openssl rand -base64 30 | tr -d "=+/" | cut -c1-25)"
    fi
}

install_coriolis_client() {
    local CORIOLIS_CLIENT_REPO="https://github.com/cloudbase/python-coriolisclient"
    local GIT_LOCAL_DIR=$(mktemp --directory --suffix="-coriolis-client")
    git clone $CORIOLIS_CLIENT_REPO "$GIT_LOCAL_DIR"
    pip install "$GIT_LOCAL_DIR"
    rm -rf $GIT_LOCAL_DIR
}
