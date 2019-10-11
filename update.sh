#!/bin/bash

set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/utils/common.sh"

NEW_TAG="$1"
if [ ! "$NEW_TAG" ]; then
    NEW_TAG=`"$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n "docker_images_tag"`
fi

# cleanup existing containers:
bash "$BASE_DIR/cleanup.sh"

# ensure we're logged into the registry:
REGISTRY=`"$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n "docker_registry"`
echo "Log in to $REGISTRY:"
docker login "$REGISTRY"

# paste in new tag:
sed -r -i.bak \
    "s/^(docker_images_tag:.*)$/docker_images_tag: $NEW_TAG/g" \
    "$BASE_DIR/config-build.yml"

# ensure coriolis-docker is configured to pull the images:
sed -r -i.bak \
    "s/^(pull_coriolis_docker_images:(.*))$/pull_coriolis_docker_images: true/g" \
    "$BASE_DIR/config.yml"

# run licensing server deployment (which will also deploy the rest):
bash "$BASE_DIR/licensing/deploy.sh"
