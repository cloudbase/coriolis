#!/bin/bash
set -e

basedir=$(dirname "$(readlink -f "$0")")

# Defaults to localhost
iface=${1:-lo}

get_config_value=$basedir/../get_config_value.py
set_config_value=$basedir/../set_config_value.py
config_file=$basedir/config.yml
parent_config_file=$basedir/../config.yml
parent_config_build_file=$basedir/../config-build.yml

ansible-playbook -v $basedir/reset-licensing-db.yml \
-e @/etc/kolla/passwords.yml \
-e @$config_file \
-e @$parent_config_file \
-e @$parent_config_build_file
