#!/bin/bash
set -e

if [ ! -d kolla ]; then
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

distro=${2:-oraclelinux}

kolla-build -b $distro -n coriolis keystone barbican rabbitmq mariadb kolla-toolbox fluentd cron memcached $push_args
