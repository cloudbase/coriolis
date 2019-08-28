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

set_config_random_value() {
    name=$1
    if [ -z $(python $get_config_value -c $config_file -n $name) ]; then
        python $set_config_value -c $config_file -n $name -v $(openssl rand 18 -base64)
    fi
}

if [ ! -f $config_file ]; then
    cp $config_file.sample $config_file
fi
if [ ! -f $parent_config_file ]; then
    cp $parent_config_file.sample $parent_config_file
fi

if [ ! -f $parent_config_build_file ]; then
    cp $parent_config_build_file.sample $parent_config_build_file
fi

set_config_random_value coriolis_licensing_ui_database_password 

# NOTE: the Kolla deployment process automatically pull in `docker-py` as the
# module to interact with Docker, though this the module is no longer supported
# by Ansible, so we must ensure we have the updated `docker` package installed.
if [ "$(pip freeze | grep docker-py==)" ]; then
    pip uninstall -y docker-py
fi
pip install --upgrade --force-reinstall docker

ansible-playbook -v $basedir/deploy.yml \
-e @/etc/kolla/passwords.yml \
-e @$config_file \
-e @$parent_config_file \
-e @$parent_config_build_file

LICENSING_UI_PORT=`python $get_config_value -c $config_file -n "coriolis_licensing_ui_server_port_external"`
LICENSING_UI_BASE_URL="http://127.0.0.1:$LICENSING_UI_PORT"
python $set_config_value -c $parent_config_file -n "licensing_ui_server_base_url" -v "$LICENSING_UI_BASE_URL"

echo "Deploying coriolis-web-proxy container"
docker rm -f coriolis-web-proxy || true
bash "$basedir/../proxy_deploy.sh"
