#!/bin/bash
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/../utils/common.sh"

LICENSING_CONFIG_FILE="$BASE_DIR/config.yml"

new_config_file "$LICENSING_CONFIG_FILE"
set_config_file_random_value "$LICENSING_CONFIG_FILE" coriolis_licensing_database_password

setup_docker_pip_package
ansible-playbook -v "$BASE_DIR/deploy.yml" -e @/etc/kolla/passwords.yml \
                 -e @"$LICENSING_CONFIG_FILE" \
                 -e @"$CONFIG_FILE" \
                 -e @"$CONFIG_BUILD_FILE"

LICENSING_SERVER_PORT=$(python "$GET_CONFIG_VALUE_SCRIPT" -c "$LICENSING_CONFIG_FILE" -n licensing_server_port_external)
LICENSING_SERVER_BASE_URL="http://127.0.0.1:$LICENSING_SERVER_PORT/v1"
python "$SET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_FILE" -n licensing_server_base_url -v "$LICENSING_SERVER_BASE_URL"

remove_docker_container "coriolis-conductor"
remove_docker_container "coriolis-web-proxy"

echo "Re-running conductor and web-proxy container deployment for licensing setup."
bash "$BASE_DIR/../deploy.sh"
