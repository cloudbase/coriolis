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

pip3 install tox
pip3 install "$BASE_DIR/kolla"

KOLLA_BUILD_CONF="$BASE_DIR/kolla/etc/kolla/kolla-build.conf"

if [ ! -f "$KOLLA_BUILD_CONF" ]; then
    pushd "$BASE_DIR/kolla"
    tox -e genconfig
    popd
fi

REGISTRY=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n docker_registry)
NAMESPACE=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n kolla_containers_namespace)
DOCKER_IMAGES_TAG=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n docker_images_tag)
OPENSTACK_RELEASE=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n kolla_openstack_release)

$SET_INI_CONFIG_VALUE_SCRIPT -c $KOLLA_BUILD_CONF -s DEFAULT -n registry -v $REGISTRY
$SET_INI_CONFIG_VALUE_SCRIPT -c $KOLLA_BUILD_CONF -s DEFAULT -n namespace -v $NAMESPACE
$SET_INI_CONFIG_VALUE_SCRIPT -c $KOLLA_BUILD_CONF -s DEFAULT -n tag -v $DOCKER_IMAGES_TAG
$SET_INI_CONFIG_VALUE_SCRIPT -c $KOLLA_BUILD_CONF -s DEFAULT -n openstack_release -v $OPENSTACK_RELEASE
$SET_INI_CONFIG_VALUE_SCRIPT -c $KOLLA_BUILD_CONF -s DEFAULT -n base -v "ubuntu"
$SET_INI_CONFIG_VALUE_SCRIPT -c $KOLLA_BUILD_CONF -s DEFAULT -n install_type -v "source"

kolla-build --config-dir "$BASE_DIR/kolla/etc/kolla" \
    keystone barbican rabbitmq mariadb kolla-toolbox fluentd cron memcached redis
