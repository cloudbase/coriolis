#!/bin/bash
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/../utils/common.sh"

new_config_file $CONFIG_FILE

KOLLA_BRANCH="$(get_global_config_value kolla_branch)"
KOLLA_OPENSTACK_RELEASE="$(get_global_config_value kolla_openstack_release)"

run_cmd_with_retry 10 10 60 pip3 install git+https://github.com/openstack/kolla@${KOLLA_BRANCH:-master}

kolla-build --registry "$(get_global_config_value docker_registry)" \
            --namespace "$(get_global_config_value docker_namespace)" \
            --tag "$(get_global_config_value docker_images_tag)" \
            --openstack-release ${KOLLA_OPENSTACK_RELEASE:-master} \
            --base "ubuntu" \
            --type "source" \
            keystone barbican rabbitmq mariadb kolla-toolbox cron memcached redis
