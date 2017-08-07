#!/bin/bash
set -e

if [ ! -d kolla ]; then
    git clone https://github.com/openstack/kolla -b stable/ocata
fi

if [ ! -d ./kolla/.venv ]; then
    virtualenv ./kolla/.venv
fi

source ./kolla/.venv/bin/activate
pip install ./kolla
pip install tox

if [ ! -f ./kolla/etc/kolla/kolla-build.conf ]; then
    pushd ./kolla
    tox -e genconfig
    popd
fi

distro=oraclelinux

# Login on the docker registry first otherwise the push will fail
# or remove --push for local testing
# docker login

kolla-build -b $distro -n coriolis keystone barbican rabbitmq mariadb kolla-toolbox fluentd cron memcached --push
