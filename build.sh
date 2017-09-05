#!/bin/bash
set -e

basedir=$(dirname "$(readlink -f "$0")")

config_file=$basedir/config-build.yml

if [ ! -f $config_file ]; then
    cp $config_file.sample $config_file
fi

ansible-playbook $basedir/build.yml -e @$config_file
