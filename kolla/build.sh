#!/bin/bash
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/../utils/common.sh"

pushd "$BASE_DIR"

if [ ! -d ./kolla ]; then
    git clone https://github.com/openstack/kolla -b stable/rocky
fi

if [ ! -d ./kolla/.venv ]; then
    virtualenv ./kolla/.venv
fi

source ./kolla/.venv/bin/activate
pip install -U pip
pip install ./kolla
pip install tox

if [ ! -f ./kolla/etc/kolla/kolla-build.conf ]; then
    pushd ./kolla
    tox -e genconfig
    popd
fi

new_config_file "$CONFIG_BUILD_FILE"

DISTRO=${1:-oraclelinux}
BUILD_ARGS=""

if [ "$2" == "push" ]; then
    BUILD_ARGS="$BUILD_ARGS --push --push-threads 4"
fi
docker_registry=$(python "$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n docker_registry)
if [ "$docker_registry" ]; then
    BUILD_ARGS="$BUILD_ARGS --registry $docker_registry"
fi
kolla_containers_namespace=$(python "$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n kolla_containers_namespace)
if [ "$kolla_containers_namespace" ]; then
    BUILD_ARGS="$BUILD_ARGS --namespace $kolla_containers_namespace"
fi
docker_images_tag=$(python "$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n docker_images_tag)
if [ "$docker_images_tag" ]; then
    BUILD_ARGS="$BUILD_ARGS --tag $docker_images_tag"
fi

kolla-build -b $DISTRO $BUILD_ARGS \
    keystone barbican rabbitmq mariadb kolla-toolbox fluentd cron memcached

popd
