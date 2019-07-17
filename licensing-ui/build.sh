#!/bin/bash
set -e

basedir=$(dirname "$(readlink -f "$0")")

config_file=$basedir/config-build.yml
parent_config_file=$basedir/../config-build.yml

# NOTE: Ansible requires that `docker-py` be installed
# instead of the newer `docker` when building images
if [ "$(pip freeze | grep docker==)" ]; then
    pip uninstall -y docker
fi
pip install --upgrade --force-reinstall docker-py

if [ ! -f $config_file ]; then
    cp $config_file.sample $config_file
fi
if [ ! -f $parent_config_file]; then
    cp $parent_config_file.sample $parent_config_file
fi

ansible-playbook -v $basedir/build.yml -e @$config_file -e @$parent_config_file

echo "Building coriolis-web-proxy container."
bash "$basedir/../proxy_build.sh"
