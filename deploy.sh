#!/bin/bash
set -e

basedir=$(dirname "$(readlink -f "$0")")

iface=${1:-ens160}

set_config_value=$basedir/set_config_value.py
config_file=$basedir/config.yml

if [ ! -f $config_file ]; then
    cp $config_file.sample $config_file
fi

VIP=$(/sbin/ip -4 -o addr show dev $iface | awk '{split($4,a,"/");print a[1]}')
python $set_config_value -c $config_file -n coriolis_host -v $VIP

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
