#!/bin/bash
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/../utils/common.sh"

new_config_file $CONFIG_FILE
new_config_file $DOCKER_IMAGES_CONFIG_FILE

KOLLA_BRANCH="$(get_global_config_value kolla_branch)"

run_cmd_with_retry 10 10 60 pip3 install -U git+https://github.com/openstack/kolla-ansible@${KOLLA_BRANCH:-stable/train}

mkdir -p /etc/kolla/
chmod 700 /etc/kolla/

if [ ! -f /etc/kolla/passwords.yml ]; then
    run_cmd_with_retry 10 10 60 curl --silent \
        https://raw.githubusercontent.com/openstack/kolla-ansible/${KOLLA_BRANCH:-stable/train}/etc/kolla/passwords.yml \
        -o /etc/kolla/passwords.yml
    kolla-genpwd
fi

KOLLA_CONF="/etc/kolla/globals.yml"
KOLLA_DOCKER_IMAGES_TAG=$(get_global_config_value kolla_docker_images_tag)
BIND_ADDRESS=$(get_global_config_value bind_address)
EXTERNAL_FQDN=$(get_global_config_value2 coriolis_certtificate_fqdn)
COMBINED_CERT=$(get_global_config_value2 coriolis_appliance_tls_combined)
CA_CERT=$(get_global_config_value2 coriolis_appliance_tls_cacert)
CORIOLIS_APPLIANCE_CERT=$(get_global_config_value2 coriolis_appliance_tls_certificate)
CORIOLIS_APPLIANCE_KEY=$(get_global_config_value2 coriolis_appliance_tls_key)

mkdir -p /etc/kolla/certificates/ca/
cp -f $CA_CERT /etc/kolla/certificates/ca/

if [ ! -z "$EXTERNAL_FQDN" ];then
    "$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_external_fqdn -v $EXTERNAL_FQDN
fi

echo "CA cert: $CA_CERT"
echo "Combined cert: $COMBINED_CERT"

# TODO(gabriel-samfira): Make this path configurable. On RHEL based systems, the target for the
# x509 certificate store is: /etc/pki/tls/certs
# The openstack_cacert will need to be adjusted as well if this changes based on base OS for kolla.
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n default_extra_volumes -v '["/etc/ssl/certs:/etc/ssl/certs:ro"]'

# The bootstrap playbook installs the step-ca root ca in the system CA store
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n openstack_cacert -v "/etc/ssl/certs/ca-certificates.crt"
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_internal_fqdn_cert -v $COMBINED_CERT
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_external_fqdn_cert -v $COMBINED_CERT
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_enable_tls_external -v 'yes'
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_enable_tls_internal -v 'yes'
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_enable_tls_backend -v 'yes'
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_tls_backend_cert -v "$CORIOLIS_APPLIANCE_CERT"
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_tls_backend_key -v "$CORIOLIS_APPLIANCE_KEY"


"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n docker_registry -v "$(get_global_config_value docker_registry)"
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n docker_namespace -v "$(get_global_config_value docker_namespace)"
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n openstack_release -v ${KOLLA_DOCKER_IMAGES_TAG:-latest}
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n network_interface -v $($UTILS_DIR/get_network_interface.py --address $BIND_ADDRESS)
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_internal_vip_address -v $BIND_ADDRESS
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_base_distro -v "ubuntu"
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n kolla_install_type -v "source"
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n enable_haproxy -v no
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n enable_fluentd -v no
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n enable_redis -v no
"$SET_CONFIG_VALUE_SCRIPT" -c $KOLLA_CONF -n enable_barbican -v yes

kolla-ansible -i "$BASE_DIR/coriolis" deploy
kolla-ansible -i "$BASE_DIR/coriolis" post-deploy

run_cmd_with_retry 10 10 60 pip3 install "python-openstackclient<=6.0.0" python-barbicanclient

source /etc/kolla/admin-openrc.sh
run_cmd_with_retry 10 10 60 openstack endpoint list
run_cmd_with_retry 10 10 60 openstack secret list

grep -q "^source /etc/kolla/admin-openrc.sh$" ~/.bashrc || echo "source /etc/kolla/admin-openrc.sh" >> ~/.bashrc
