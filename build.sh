#!/bin/bash
set -e

basedir=$(dirname "$(readlink -f "$0")")

ansible-playbook $basedir/build.yml
