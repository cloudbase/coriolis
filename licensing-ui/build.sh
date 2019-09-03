#!/bin/bash
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/../utils/common.sh"

LICENSING_UI_CONFIG_BUILD_FILE="$BASE_DIR/config-build.yml"

new_config_file "$LICENSING_UI_CONFIG_BUILD_FILE"
new_config_file "$CONFIG_BUILD_FILE"

setup_docker_py_pip_package
ansible-playbook -v "$BASE_DIR/build.yml" \
                 -e "@$LICENSING_UI_CONFIG_BUILD_FILE" -e @"$CONFIG_BUILD_FILE"

bash "$BASE_DIR/../proxy_build.sh"
