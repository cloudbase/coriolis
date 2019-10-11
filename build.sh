#!/bin/bash
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/utils/common.sh"

new_config_file "$CONFIG_BUILD_FILE"

ansible-playbook -v "$BASE_DIR/build.yml" -e @"$CONFIG_BUILD_FILE"
bash "$BASE_DIR/proxy_build.sh"
