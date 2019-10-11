#!/bin/bash
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/../utils/common.sh"

LICENSING_CONFIG_BUILD_FILE="$BASE_DIR/config-build.yml"

new_config_file "$LICENSING_CONFIG_BUILD_FILE"

ansible-playbook -v "$BASE_DIR/build.yml" \
                 -e @"$LICENSING_CONFIG_BUILD_FILE" -e @"$CONFIG_BUILD_FILE"
