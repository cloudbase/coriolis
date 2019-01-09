#!/bin/bash

basedir=$(dirname "$(readlink -f "$0")")
if [ -d /root/kolla-ansible ]; then
    if [ -d $basedir/kolla-ansible ]; then
        echo "ERROR: 'kolla-ansible' directory found both in /root and $basedir."
        echo "Please pick the latest one used and move it in $basedir."
        exit 2
    fi
    echo "WARN: old kolla-ansible found in /root, moving it to $basedir."
    mv /root/kolla-ansible $basedir
fi
pushd "$basedir"

kolla-ansible destroy --yes-i-really-really-mean-it ./kolla-ansible/ansible/inventory/coriolis

docker volume rm mariadb
docker volume rm rabbitmq
docker volume rm kolla_logs

rm -rf /etc/kolla/
