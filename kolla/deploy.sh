#!/bin/bash
set -e

basedir=$(dirname "$(readlink -f "$0")")
if [ -d /root/kolla-ansible ]; then
    if [ -d $basedir/kolla-ansible ]; then
        echo "ERROR: 'kolla-ansible' directory found both in /root and $basedir."
        echo "Please pick the latest one used and move it in $basedir."
        exit 2
    fi
    echo "WARN: old kolla-ansible found in /root, moving it to $basedir."
    mv /root/kolla-ansible $basedir
fi
pushd "$basedir"

distro=${1:-oraclelinux}
# Defaults to localhost
iface=${2:-lo}

if [ ! -d ./kolla-ansible ]; then
    git clone https://github.com/openstack/kolla-ansible -b stable/ocata

    pip install -q ./kolla-ansible
fi

mkdir -p /etc/kolla/
chmod 700 /etc/kolla/

if [ ! -f /etc/kolla/passwords.yml ]; then
    cp ./kolla-ansible/etc/kolla/passwords.yml /etc/kolla/
    kolla-genpwd
fi

cp ./coriolis ./kolla-ansible/ansible/inventory/

set_config_value=../set_config_value.py

VIP=$(/sbin/ip -4 -o addr show dev $iface | awk '{split($4,a,"/");print a[1]}')
if [ ! "$VIP" ]; then
    echo "Could not find IP for interface $iface"
    exit 1
fi
python $set_config_value -c /etc/kolla/globals.yml -n kolla_internal_vip_address -v $VIP
python $set_config_value -c /etc/kolla/globals.yml -n network_interface -v $iface
python $set_config_value -c /etc/kolla/globals.yml -n kolla_base_distro -v $distro

config_build_file=../config-build.yml
if [ ! -e "$config_build_file" ]; then
    cp $config_build_file.sample $config_build_file
fi
get_config_value=../get_config_value.py
kolla_containers_namespace=`python $get_config_value -c $config_build_file -n kolla_containers_namespace`
if [ "$kolla_containers_namespace" ]; then
    python $set_config_value -c /etc/kolla/globals.yml -n docker_namespace -v "$kolla_containers_namespace"
fi

config_build_file=../config-build.yml
if [ ! -f $config_build_file ]; then
    cp $config_build_file.sample $config_build_file
fi

docker_registry=`python $get_config_value -c $config_build_file -n docker_registry`
if [ "$docker_registry" ]; then
    python $set_config_value -c /etc/kolla/globals.yml -n docker_registry -v $docker_registry
fi

python $set_config_value -c /etc/kolla/globals.yml -n enable_barbican -v yes

kolla-ansible deploy -i ./kolla-ansible/ansible/inventory/coriolis
kolla-ansible post-deploy

pip install -q python-openstackclient
pip install -q python-barbicanclient
source /etc/kolla/admin-openrc.sh
openstack endpoint list
openstack secret list

grep -q "^source /etc/kolla/admin-openrc.sh$" ~/.bashrc || echo "source /etc/kolla/admin-openrc.sh" >> ~/.bashrc

popd
