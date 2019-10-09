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

if [ ! -d "$BASE_DIR/kolla-ansible/.venv" ]; then
    python3 -m virtualenv "$BASE_DIR/kolla-ansible/.venv"
fi

source "$BASE_DIR/kolla-ansible/.venv/bin/activate"

pip3 install "$BASE_DIR/kolla-ansible"

mkdir -p /etc/kolla/
chmod 700 /etc/kolla/

if [ ! -f /etc/kolla/passwords.yml ]; then
    cp "$BASE_DIR/kolla-ansible/etc/kolla/passwords.yml" /etc/kolla/
    kolla-genpwd
fi

DISTRO=${1:-centos}
IFACE=${2:-lo} # Defaults to localhost

VIP=$(/sbin/ip -4 -o addr show dev $IFACE | awk '{split($4,a,"/");print a[1]}')
if [ ! "$VIP" ]; then
    echo "Could not find IP for interface $IFACE"
    exit 1
fi

"$SET_CONFIG_VALUE_SCRIPT" -c /etc/kolla/globals.yml -n kolla_internal_vip_address -v $VIP
"$SET_CONFIG_VALUE_SCRIPT" -c /etc/kolla/globals.yml -n network_interface -v $IFACE
"$SET_CONFIG_VALUE_SCRIPT" -c /etc/kolla/globals.yml -n kolla_base_distro -v $DISTRO

kolla_containers_namespace=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n kolla_containers_namespace)
if [ "$kolla_containers_namespace" ]; then
    "$SET_CONFIG_VALUE_SCRIPT" -c /etc/kolla/globals.yml -n docker_namespace -v "$kolla_containers_namespace"
fi

docker_registry=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n docker_registry)
if [ "$docker_registry" ]; then
    "$SET_CONFIG_VALUE_SCRIPT" -c /etc/kolla/globals.yml -n docker_registry -v $docker_registry
fi

docker_images_tag=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n docker_images_tag)
if [ "$docker_images_tag" ]; then
    "$SET_CONFIG_VALUE_SCRIPT" -c /etc/kolla/globals.yml -n openstack_release -v $docker_images_tag
fi

"$SET_CONFIG_VALUE_SCRIPT" -c /etc/kolla/globals.yml -n enable_barbican -v yes
"$SET_CONFIG_VALUE_SCRIPT" -c /etc/kolla/globals.yml -n enable_redis -v yes

kolla-ansible -i "$BASE_DIR/coriolis" deploy
kolla-ansible -i "$BASE_DIR/coriolis" post-deploy

innodb_log_file_size=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_FILE" -n mariadb_innodb_log_file_size)
if [ "$innodb_log_file_size" ]; then
    $SET_INI_CONFIG_VALUE_SCRIPT -c /etc/kolla/mariadb/galera.cnf -s mysqld -n innodb_log_file_size -v $innodb_log_file_size
    docker restart mariadb
fi

deactivate

pip3 install python-openstackclient python-barbicanclient

source /etc/kolla/admin-openrc.sh
openstack endpoint list
openstack secret list

grep -q "^source /etc/kolla/admin-openrc.sh$" ~/.bashrc || echo "source /etc/kolla/admin-openrc.sh" >> ~/.bashrc
