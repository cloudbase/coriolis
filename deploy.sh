#!/bin/bash
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/utils/common.sh"

IFACE=${1:-lo} # Defaults to localhost
VIP=$(/sbin/ip -4 -o addr show dev $IFACE | awk '{split($4,a,"/");print a[1]}')

new_config_file "$CONFIG_FILE"
new_config_file "$CONFIG_BUILD_FILE"

python "$SET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_FILE" -n coriolis_host -v $VIP
set_config_file_random_value "$CONFIG_FILE" coriolis_database_password
set_config_file_random_value "$CONFIG_FILE" coriolis_keystone_password
set_config_file_random_value "$CONFIG_FILE" temp_keypair_password

setup_docker_pip_package
ansible-playbook -v "$BASE_DIR/deploy.yml" -e @"/etc/kolla/passwords.yml" \
                 -e @"$CONFIG_FILE" -e @"$CONFIG_BUILD_FILE"

bash "$BASE_DIR/deploy_worker.sh" "$VIP"
bash "$BASE_DIR/proxy_deploy.sh"

install_coriolis_client
source /etc/kolla/admin-openrc.sh
coriolis endpoint list
