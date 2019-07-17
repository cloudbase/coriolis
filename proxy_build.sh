#!/bin/bash
set -e

basedir=$(dirname "$(readlink -f "$0")")

config_file=$basedir/config-build.yml

# NOTE: Ansible requires that `docker-py` be installed
# instead of the newer `docker` when building images
if [ "$(pip freeze | grep docker==)" ]; then
    pip uninstall -y docker
fi
pip install --upgrade --force-reinstall docker-py

if [ ! -f $config_file ]; then
    cp $config_file.sample $config_file
fi

ansible-playbook -v $basedir/proxy_build.yml -e @$config_file
