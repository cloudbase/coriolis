#!/bin/bash
set -e

basedir=$(dirname "$(readlink -f "$0")")

# Defaults to localhost
iface=${1:-lo}

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
    cp $config_file.sample $config_file
fi

if [ ! -f $config_build_file ]; then
    cp $config_build_file.sample $config_build_file
fi

VIP=$(/sbin/ip -4 -o addr show dev $iface | awk '{split($4,a,"/");print a[1]}')
python $set_config_value -c $config_file -n coriolis_host -v $VIP

set_config_random_value coriolis_database_password
set_config_random_value coriolis_keystone_password
set_config_random_value temp_keypair_password

ansible-playbook $basedir/deploy.yml \
-e @/etc/kolla/passwords.yml \
-e @$config_file \
-e @$basedir/config-build.yml

if [ ! -d python-coriolisclient ]; then
    git clone https://github.com/cloudbase/python-coriolisclient
fi
pip install -q ./python-coriolisclient

source /etc/kolla/admin-openrc.sh
coriolis endpoint list
