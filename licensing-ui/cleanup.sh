#!/bin/bash
# USAGE: $0 IMAGE_DOCKER_TAG IMAGE_DOCKER_REPO
# EX: $0 1.0.1-dev registry.cloudbase.it
set -e

if [ ! "$1" ] || [ ! "$2" ]; then
    echo "USAGE: $0 IMAGE_DOCKER_TAG IMAGE_DOCKER_REPO"
    exit 1
fi

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/../utils/common.sh"

remove_docker_container "coriolis-licensing-ui"
remove_docker_image "$2/coriolis-licensing-ui:$1"
