#!/bin/bash
set -e

VIP="$1"
if [ $# -ne 1 ] || [ ! "$VIP" ]; then
    echo "USAGE: $0 <IP OF CORIOLIS CONTROLLER (hosting RabbitMQ server)>"
    echo "IP address of Coriolis host must be provided for deploying Coriolis worker component."
    echo "If deploying an all-in-one, using '127.0.0.1' should do fine."
    exit 1
fi

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/utils/common.sh"

echo "Deploying Coriolis worker component configured to use Controller host IP $VIP"

new_config_file "$CONFIG_FILE"
new_config_file "$CONFIG_BUILD_FILE"
set_config_file_random_value "$CONFIG_FILE" temp_keypair_password

KOLLA_PASSWORDS_FILE="/etc/kolla/passwords.yml"
if [ ! -e "$KOLLA_PASSWORDS_FILE" ]; then
    echo "$KOLLA_PASSWORDS_FILE must exist in order to deploy Coriolis worker component!"
    exit 1
fi

setup_docker_pip_package
ansible-playbook -v "$BASE_DIR/deploy_worker.yml" -e @"$KOLLA_PASSWORDS_FILE" \
                 -e @"$CONFIG_FILE" -e @"$CONFIG_BUILD_FILE"
