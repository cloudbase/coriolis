#!/bin/bash
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/../utils/common.sh"

new_config_file "$CONFIG_BUILD_FILE"

if [ ! -d "$BASE_DIR/kolla" ]; then
    kolla_branch=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n kolla_branch)
    if [ ! "$kolla_branch" ]; then
        echo "ERROR: Empty 'kolla_branch' build config option"
        exit 1
    fi
    git clone https://github.com/openstack/kolla -b $kolla_branch "$BASE_DIR/kolla"
fi

if [ ! -d "$BASE_DIR/kolla/.venv" ]; then
    python3 -m virtualenv "$BASE_DIR/kolla/.venv"
fi

source "$BASE_DIR/kolla/.venv/bin/activate"

pip3 install "$BASE_DIR/kolla"

BUILD_ARGS=""
if [ "$2" == "push" ]; then
    BUILD_ARGS="$BUILD_ARGS --push --push-threads 4"
fi
docker_registry=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n docker_registry)
if [ "$docker_registry" ]; then
    BUILD_ARGS="$BUILD_ARGS --registry $docker_registry"
fi
kolla_containers_namespace=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n kolla_containers_namespace)
if [ "$kolla_containers_namespace" ]; then
    BUILD_ARGS="$BUILD_ARGS --namespace $kolla_containers_namespace"
fi
docker_images_tag=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n docker_images_tag)
if [ "$docker_images_tag" ]; then
    BUILD_ARGS="$BUILD_ARGS --tag $docker_images_tag"
fi

DISTRO=${1:-centos}

kolla-build -b $DISTRO $BUILD_ARGS \
    keystone barbican rabbitmq mariadb kolla-toolbox fluentd cron memcached redis
