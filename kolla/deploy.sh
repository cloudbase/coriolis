#!/bin/bash
set -e

basedir=$(dirname "$(readlink -f "$0")")

iface=${1:-ens160}
distro=oraclelinux

if [ ! -d kolla-ansible ]; then
    git clone https://github.com/openstack/kolla-ansible -b stable/ocata

    pushd ./kolla-ansible
    # Fix for barbican config issue
    git cherry-pick 2e4359069e8a50f83fe0dca1103d935212dd2703
    popd

    pip install -q ./kolla-ansible
fi

mkdir -p /etc/kolla/
chmod 700 /etc/kolla/

if [ ! -f /etc/kolla/passwords.yml ]; then
    cp kolla-ansible/etc/kolla/passwords.yml /etc/kolla/
    kolla-genpwd
fi

cp $basedir/coriolis kolla-ansible/ansible/inventory/

set_config_value=$basedir/../set_config_value.py

VIP=$(/sbin/ip -4 -o addr show dev $iface | awk '{split($4,a,"/");print a[1]}')
python $set_config_value -c /etc/kolla/globals.yml -n kolla_internal_vip_address -v $VIP
python $set_config_value -c /etc/kolla/globals.yml -n network_interface -v $iface
python $set_config_value -c /etc/kolla/globals.yml -n kolla_base_distro -v $distro
python $set_config_value -c /etc/kolla/globals.yml -n docker_namespace -v coriolis

kolla-ansible deploy -i ./kolla-ansible/ansible/inventory/coriolis
kolla-ansible post-deploy

pip install -q python-openstackclient
source /etc/kolla/admin-openrc.sh
openstack endpoint list
openstack secret list
