#!/bin/bash
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/../utils/common.sh"

LICENSING_UI_CONFIG_FILE="$BASE_DIR/config.yml"

ansible-playbook -v "$BASE_DIR/reset-licensing-ui-db.yml" \
                 -e @/etc/kolla/passwords.yml \
                 -e @"$LICENSING_UI_CONFIG_FILE" \
                 -e @"$CONFIG_FILE" \
                 -e @"$CONFIG_BUILD_FILE"
