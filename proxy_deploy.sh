#!/bin/bash
set -e

basedir=$(dirname "$(readlink -f "$0")")

# Defaults to localhost
iface=${1:-lo}

get_config_value=$basedir/get_config_value.py
set_config_value=$basedir/set_config_value.py
config_file=$basedir/config.yml
config_build_file=$basedir/config-build.yml

if [ ! -f $config_file ]; then
    cp $config_file.sample $config_file
fi

if [ ! -f $config_build_file ]; then
    cp $config_build_file.sample $config_build_file
fi

# NOTE: the Kolla deployment process automatically pull in `docker-py` as the
# module to interact with Docker, though this the module is no longer supported
# by Ansible, so we must ensure we have the updated `docker` package installed.
if [ "$(pip freeze | grep docker-py==)" ]; then
    pip uninstall -y docker-py
fi
pip install --upgrade --force-reinstall docker

ansible-playbook -v $basedir/proxy_deploy.yml \
-e @$config_file \
-e @$basedir/config-build.yml
