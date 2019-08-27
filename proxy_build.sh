#!/bin/bash
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/utils/common.sh"

echo "Building Coriolis web proxy component"

new_config_file "$CONFIG_BUILD_FILE"

setup_docker_py_pip_package
ansible-playbook -v "$BASE_DIR/proxy_build.yml" -e @"$CONFIG_BUILD_FILE"
