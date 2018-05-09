#!/bin/bash
set -e

basedir=$(dirname "$(readlink -f "$0")")

distro=${1:-oraclelinux}
# Defaults to localhost
iface=${2:-lo}

if [ ! -d kolla-ansible ]; then
    git clone https://github.com/openstack/kolla-ansible -b stable/ocata

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

python $set_config_value -c /etc/kolla/globals.yml -n enable_barbican -v yes

kolla-ansible deploy -i ./kolla-ansible/ansible/inventory/coriolis
kolla-ansible post-deploy

pip install -q python-openstackclient
pip install -q python-barbicanclient
source /etc/kolla/admin-openrc.sh
openstack endpoint list
openstack secret list

grep -q "^source /etc/kolla/admin-openrc.sh$" ~/.bashrc || echo "source /etc/kolla/admin-openrc.sh" >> ~/.bashrc
