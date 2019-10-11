#!/bin/bash
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/../utils/common.sh"

if [ -d /root/kolla-ansible ]; then
    if [ -d "$BASE_DIR/kolla-ansible" ]; then
        echo "ERROR: 'kolla-ansible' directory found both in /root and $BASE_DIR."
        echo "Please pick the latest one used and move it in $BASE_DIR."
        exit 2
    fi
    echo "WARN: old kolla-ansible found in /root, moving it to $BASE_DIR."
    mv /root/kolla-ansible "$BASE_DIR"
fi

source "$BASE_DIR/kolla-ansible/.venv/bin/activate"

kolla-ansible destroy --yes-i-really-really-mean-it ./kolla-ansible/ansible/inventory/coriolis

remove_docker_volume "mariadb"
remove_docker_volume "rabbitmq"
remove_docker_volume "kolla_logs"

rm -rf /etc/kolla/
