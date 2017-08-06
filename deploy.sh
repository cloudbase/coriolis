#!/bin/bash
set -e
kolla_internal_vip_address=$1
ansible-playbook deploy.yml -e @/etc/kolla/passwords.yml \
-e "kolla_internal_vip_address=$kolla_internal_vip_address"