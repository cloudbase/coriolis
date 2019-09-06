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

pushd "$BASE_DIR"

DISTRO=${1:-oraclelinux}
IFACE=${2:-lo} # Defaults to localhost

if [ ! -d ./kolla-ansible ]; then
    git clone https://github.com/openstack/kolla-ansible -b stable/rocky
    pip install -q ./kolla-ansible
fi

mkdir -p /etc/kolla/
chmod 700 /etc/kolla/

if [ ! -f /etc/kolla/passwords.yml ]; then
    cp ./kolla-ansible/etc/kolla/passwords.yml /etc/kolla/
    kolla-genpwd
fi

cp ./coriolis ./kolla-ansible/ansible/inventory/

VIP=$(/sbin/ip -4 -o addr show dev $IFACE | awk '{split($4,a,"/");print a[1]}')
if [ ! "$VIP" ]; then
    echo "Could not find IP for interface $IFACE"
    exit 1
fi
python $SET_CONFIG_VALUE_SCRIPT -c /etc/kolla/globals.yml -n kolla_internal_vip_address -v $VIP
python $SET_CONFIG_VALUE_SCRIPT -c /etc/kolla/globals.yml -n network_interface -v $IFACE
python $SET_CONFIG_VALUE_SCRIPT -c /etc/kolla/globals.yml -n kolla_base_distro -v $DISTRO

new_config_file "$CONFIG_BUILD_FILE"

kolla_containers_namespace=$(python "$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n kolla_containers_namespace)
if [ "$kolla_containers_namespace" ]; then
    python $SET_CONFIG_VALUE_SCRIPT -c /etc/kolla/globals.yml -n docker_namespace -v "$kolla_containers_namespace"
fi

docker_registry=$(python "$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n docker_registry)
if [ "$docker_registry" ]; then
    python $SET_CONFIG_VALUE_SCRIPT -c /etc/kolla/globals.yml -n docker_registry -v $docker_registry
fi

docker_images_tag=$(python "$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_BUILD_FILE" -n docker_images_tag)
if [ "$docker_images_tag" ]; then
    python $SET_CONFIG_VALUE_SCRIPT -c /etc/kolla/globals.yml -n openstack_release -v $docker_images_tag
fi

python $SET_CONFIG_VALUE_SCRIPT -c /etc/kolla/globals.yml -n enable_barbican -v yes

setup_docker_pip_package
kolla-ansible deploy -i ./kolla-ansible/ansible/inventory/coriolis
kolla-ansible post-deploy

pip install -U -q git+https://github.com/openstack/python-openstackclient@stable/rocky
pip install -U -q git+https://github.com/openstack/python-barbicanclient@stable/rocky
source /etc/kolla/admin-openrc.sh
openstack endpoint list
openstack secret list

grep -q "^source /etc/kolla/admin-openrc.sh$" ~/.bashrc || echo "source /etc/kolla/admin-openrc.sh" >> ~/.bashrc

popd
