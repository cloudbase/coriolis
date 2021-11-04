#!/bin/bash
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/../utils/common.sh"

new_config_file $CONFIG_FILE
new_config_file $DOCKER_IMAGES_CONFIG_FILE

KOLLA_BRANCH="$(get_global_config_value kolla_branch)"

run_cmd_with_retry 10 10 60 pip3 install -U git+https://github.com/openstack/kolla-ansible@${KOLLA_BRANCH:-stable/train}

mkdir -p /etc/kolla/
chmod 700 /etc/kolla/

if [ ! -f /etc/kolla/passwords.yml ]; then
    run_cmd_with_retry 10 10 60 curl --silent \
        https://raw.githubusercontent.com/openstack/kolla-ansible/${KOLLA_BRANCH:-stable/train}/etc/kolla/passwords.yml \
        -o /etc/kolla/passwords.yml
    kolla-genpwd
fi

KOLLA_CONF="/etc/kolla/globals.yml"
KOLLA_DOCKER_IMAGES_TAG=$(get_global_config_value kolla_docker_images_tag)
BIND_ADDRESS=$(get_global_config_value bind_address)

"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n docker_registry -v "$(get_global_config_value docker_registry)"
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n docker_namespace -v "$(get_global_config_value docker_namespace)"
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n openstack_release -v ${KOLLA_DOCKER_IMAGES_TAG:-latest}
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n network_interface -v $($UTILS_DIR/get_network_interface.py --address $BIND_ADDRESS)
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_internal_vip_address -v $BIND_ADDRESS
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_base_distro -v "ubuntu"
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_install_type -v "source"
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n enable_haproxy -v no
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n enable_fluentd -v no
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n enable_redis -v no
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n enable_barbican -v yes

kolla-ansible -i "$BASE_DIR/coriolis" deploy
kolla-ansible -i "$BASE_DIR/coriolis" post-deploy

run_cmd_with_retry 10 10 60 pip3 install python-openstackclient python-barbicanclient

source /etc/kolla/admin-openrc.sh
run_cmd_with_retry 10 10 60 openstack endpoint list
run_cmd_with_retry 10 10 60 openstack secret list

grep -q "^source /etc/kolla/admin-openrc.sh$" ~/.bashrc || echo "source /etc/kolla/admin-openrc.sh" >> ~/.bashrc
