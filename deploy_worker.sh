#!/bin/bash
set -e

VIP="$1"
if [ $# -ne 1 ] || [ ! "$VIP" ]; then
    echo "USAGE: $0 <IP OF CORIOLIS CONTROLLER (hosting RabbitMQ server)>"
    echo "IP address of Coriolis host must be provided for deploying Coriolis worker component."
    echo "If deploying an all-in-one, using '127.0.0.1' should do fine."
    exit 1
fi

basedir=$(dirname "$(readlink -f "$0")")

get_config_value=$basedir/get_config_value.py
set_config_value=$basedir/set_config_value.py
config_file=$basedir/config.yml
config_build_file=$basedir/config-build.yml

set_config_random_value() {
    name=$1
    if [ -z $(python $get_config_value -c $config_file -n $name) ]; then
        python $set_config_value -c $config_file -n $name -v $(openssl rand 18 -base64)
    fi
}

if [ ! -f $config_file ]; then
    echo "$config_file must be present for deploying Coriolis worker component!"
    exit 1
fi

if [ ! -f $config_build_file ]; then
    echo "$config_build_file must be present for deploying Coriolis worker component!"
    exit 2
fi

set_config_random_value temp_keypair_password

# NOTE: the Kolla deployment process automatically pull in `docker-py` as the
# module to interact with Docker, though this the module is no longer supported
# by Ansible, so we must ensure we have the updated `docker` package installed.
if [ "$(pip freeze | grep docker-py==)" ]; then
    pip uninstall -y docker-py
fi
pip install --upgrade --force-reinstall docker

kolla_passwords_file="/etc/kolla/passwords.yml"
if [ ! -e "$kolla_passwords_file" ]; then
    echo "$kolla_passwords_file must exist in order to deploy Coriolis worker component!"
    exit 3
fi

ansible-playbook -v $basedir/deploy_worker.yml \
-e @$kolla_passwords_file \
-e @$config_file \
-e @$basedir/config-build.yml
