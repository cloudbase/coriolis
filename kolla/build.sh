#!/bin/bash
set -e

basedir=$(dirname "$(readlink -f "$0")")
pushd "$basedir"

if [ ! -d ./kolla ]; then
    git clone https://github.com/openstack/kolla -b stable/ocata
fi

if [ ! -d ./kolla/.venv ]; then
    virtualenv ./kolla/.venv
fi

source ./kolla/.venv/bin/activate
pip install -U pip
pip install ./kolla
pip install tox

if [ ! -f ./kolla/etc/kolla/kolla-build.conf ]; then
    pushd ./kolla
    tox -e genconfig
    popd
fi

if [ "$1" == "push" ]; then
    push_args="--push --push-threads 4"
else
    push_args=""
fi


get_config_value=../get_config_value.py
config_build_file=../config-build.yml
if [ ! -f $config_build_file ]; then
    cp $config_build_file.sample $config_build_file
fi

docker_registry=`python $get_config_value -c $config_build_file -n docker_registry`
if [ "$docker_registry" ]; then
    push_args="$push_args --registry $docker_registry"
fi

kolla_containers_namespace=`python $get_config_value -c $config_build_file -n kolla_containers_namespace`
if [ "$kolla_containers_namespace" ]; then
    push_args="$push_args -n $kolla_containers_namespace"
fi

distro=${2:-oraclelinux}

kolla-build -b $distro $push_args \
    keystone barbican rabbitmq mariadb kolla-toolbox fluentd cron memcached

popd
