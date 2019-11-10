#!/bin/bash
set -e

BASE_DIR=$(dirname "$(readlink -f "$0")")

kolla-ansible destroy --yes-i-really-really-mean-it -e "destroy_include_images=true" "$BASE_DIR/coriolis"

sed -i "/source \/etc\/kolla\/admin-openrc.sh/d" ~/.bashrc
