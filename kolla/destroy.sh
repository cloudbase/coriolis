#!/bin/bash

kolla-ansible destroy --yes-i-really-really-mean-it ./kolla-ansible/ansible/inventory/coriolis

docker volume rm mariadb
docker volume rm rabbitmq
docker volume rm kolla_logs

rm -rf /etc/kolla/
