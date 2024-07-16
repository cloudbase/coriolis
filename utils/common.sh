#!/bin/bash

UTILS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GET_CONFIG_VALUE_SCRIPT="$UTILS_DIR/get_config_value.py"
SET_CONFIG_VALUE_SCRIPT="$UTILS_DIR/set_config_value.py"
SET_INI_CONFIG_VALUE_SCRIPT="$UTILS_DIR/set_ini_config_value.py"
CONFIG_FILE=$(readlink -f "$UTILS_DIR/../config.yml")
ALL_VARS_FILE=$(readlink -f "$UTILS_DIR/../coriolis_ansible/group_vars/all.yml")

DOCKER_IMAGES_CONFIG_FILE=$(readlink -f "$UTILS_DIR/../docker-images-config.yml")
PASSWORDS_FILE=$(readlink -f "$UTILS_DIR/../passwords.yml")
VARS_AS_JSON=$(ANSIBLE_LOAD_CALLBACK_PLUGINS=1 \
    ANSIBLE_CALLBACK_WHITELIST=json \
    ANSIBLE_STDOUT_CALLBACK=json \
    ansible localhost -m debug \
    -a var='hostvars["localhost"]' \
    -e @$CONFIG_FILE \
    -e @$ALL_VARS_FILE | jq '.plays[0].tasks[0].hosts.localhost."hostvars[\"localhost\"]"')

new_config_file() {
    local CONF_FILE_PATH="$1"
    if [[ ! -f "${CONF_FILE_PATH}.sample" ]]; then
        echo "Sample config file does not exist"
        echo "Cannot create or update the config file: $CONF_FILE_PATH"
        return 1
    fi
    if [[ -f "$CONF_FILE_PATH" ]]; then
        "$UTILS_DIR/update_config_file.py" --config-file "$CONF_FILE_PATH"
        return
    fi
    cp "${CONF_FILE_PATH}.sample" "$CONF_FILE_PATH"
}

get_global_config_value2() {
    local NAME="$1"

    local VALUE=$(echo "$VARS_AS_JSON" | jq -r ".$NAME")
    if [[ "$VALUE" == "null" ]]; then
        VALUE=""
    fi
    echo "$VALUE"
}

get_global_config_value() {
    local NAME="$1"
    # Check in main config file:
    local CONFIG_VALUE=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_FILE" -n "$NAME")

    # Check Docker images config file:
    if [[ -z $CONFIG_VALUE ]]; then
        CONFIG_VALUE=$("$GET_CONFIG_VALUE_SCRIPT" -c "$DOCKER_IMAGES_CONFIG_FILE" -n "$NAME")
    fi

    # Default to whatever's in group_vars/all.yml:
    if [[ -z $CONFIG_VALUE ]]; then
        CONFIG_VALUE=$("$GET_CONFIG_VALUE_SCRIPT" -c "$ALL_VARS_FILE" -n "$NAME")
    fi

    echo $CONFIG_VALUE
}

run_cmd_with_retry() {
    local RETRIES=$1
    local WAIT_SLEEP=$2
    local TIMEOUT=$3

    shift && shift && shift

    for i in $(seq 1 $RETRIES); do
        timeout $TIMEOUT ${@} && break || \
        if [ $i -eq $RETRIES ]; then
            echo "Error: Failed to execute \"$@\" after $i attempts"
            return 1
        else
            echo "Failed to execute \"$@\". Retrying in $WAIT_SLEEP seconds..."
            sleep $WAIT_SLEEP
        fi
    done
    echo Executed \"$@\" $i times;
}
