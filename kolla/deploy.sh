#!/bin/bash
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/../utils/common.sh"

new_config_file "$CONFIG_FILE"
new_config_file "$CONFIG_BUILD_FILE"

if [ -d /root/kolla-ansible ]; then
    if [ -d "$BASE_DIR/kolla-ansible" ]; then
        echo "ERROR: 'kolla-ansible' directory found both in /root and $BASE_DIR."
        echo "Please pick the latest one used and move it in $BASE_DIR."
        exit 2
    fi
    echo "WARN: old kolla-ansible found in /root, moving it to $BASE_DIR."
    mv /root/kolla-ansible "$BASE_DIR"
fi

if [ ! -d "$BASE_DIR/kolla-ansible" ]; then
    kolla_branch=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n kolla_branch)
    if [ ! "$kolla_branch" ]; then
        echo "ERROR: Empty 'kolla_branch' config-build.yml option"
        exit 1
    fi
    git clone https://github.com/openstack/kolla-ansible -b $kolla_branch "$BASE_DIR/kolla-ansible"
fi

pip3 install "$BASE_DIR/kolla-ansible"

mkdir -p /etc/kolla/
chmod 700 /etc/kolla/

if [ ! -f /etc/kolla/passwords.yml ]; then
    cp "$BASE_DIR/kolla-ansible/etc/kolla/passwords.yml" /etc/kolla/
    kolla-genpwd
fi

IFACE=${1:-lo} # Defaults to localhost

VIP=$(/sbin/ip -4 -o addr show dev $IFACE | awk '{split($4,a,"/");print a[1]}')
if [ ! "$VIP" ]; then
    echo "Could not find IP for interface $IFACE"
    exit 1
fi

KOLLA_CONF="/etc/kolla/globals.yml"

REGISTRY=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n docker_registry)
NAMESPACE=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n kolla_containers_namespace)
DOCKER_IMAGES_TAG=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n docker_images_tag)

"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n docker_registry -v $REGISTRY
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n docker_namespace -v $NAMESPACE
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n openstack_release -v $DOCKER_IMAGES_TAG
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n enable_barbican -v yes
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n enable_redis -v yes
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_base_distro -v "ubuntu"
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_install_type -v "source"
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n network_interface -v $IFACE
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_internal_vip_address -v $VIP

kolla-ansible -i "$BASE_DIR/coriolis" deploy
kolla-ansible -i "$BASE_DIR/coriolis" post-deploy

innodb_log_file_size=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_FILE" -n mariadb_innodb_log_file_size)
if [ "$innodb_log_file_size" ]; then
    $SET_INI_CONFIG_VALUE_SCRIPT -c /etc/kolla/mariadb/galera.cnf -s mysqld -n innodb_log_file_size -v $innodb_log_file_size
    docker restart mariadb
fi

pip3 install python-openstackclient python-barbicanclient

source /etc/kolla/admin-openrc.sh
run_cmd_with_retry 10 10 60 openstack endpoint list
run_cmd_with_retry 10 10 60 openstack secret list

grep -q "^source /etc/kolla/admin-openrc.sh$" ~/.bashrc || echo "source /etc/kolla/admin-openrc.sh" >> ~/.bashrc
