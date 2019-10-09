#!/bin/bash
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/../utils/common.sh"

LICENSING_UI_CONFIG_FILE="$BASE_DIR/config.yml"

new_config_file "$LICENSING_UI_CONFIG_FILE"
new_config_file "$CONFIG_FILE"
new_config_file "$CONFIG_BUILD_FILE"
set_config_file_random_value "$LICENSING_UI_CONFIG_FILE" coriolis_licensing_ui_database_password

ansible-playbook -v "$BASE_DIR/deploy.yml" -e @/etc/kolla/passwords.yml \
                 -e @"$LICENSING_UI_CONFIG_FILE" \
                 -e @"$CONFIG_FILE" \
                 -e @"$CONFIG_BUILD_FILE"

LICENSING_UI_PORT=$("$GET_CONFIG_VALUE_SCRIPT" -c "$LICENSING_UI_CONFIG_FILE" -n coriolis_licensing_ui_server_port_external)
LICENSING_UI_BASE_URL="http://127.0.0.1:$LICENSING_UI_PORT/licensing-ui"
"$SET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_FILE" -n licensing_ui_server_base_url -v "$LICENSING_UI_BASE_URL"

remove_docker_container "coriolis-web-proxy"

echo "Re-running web-proxy container deployment for licensing UI setup."
bash "$BASE_DIR/../proxy_deploy.sh"
