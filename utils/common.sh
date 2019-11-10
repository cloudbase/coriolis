#!/bin/bash

UTILS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GET_CONFIG_VALUE_SCRIPT="$UTILS_DIR/get_config_value.py"
SET_CONFIG_VALUE_SCRIPT="$UTILS_DIR/set_config_value.py"
SET_INI_CONFIG_VALUE_SCRIPT="$UTILS_DIR/set_ini_config_value.py"
CONFIG_FILE=$(readlink -f "$UTILS_DIR/../config.yml")
PASSWORDS_FILE=$(readlink -f "$UTILS_DIR/../passwords.yml")


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

get_global_config_value() {
    local NAME="$1"
    local CONFIG_VALUE=$("$GET_CONFIG_VALUE_SCRIPT" -c "$CONFIG_FILE" -n "$NAME")
    if [[ -z $CONFIG_VALUE ]]; then
        CONFIG_VALUE=$("$GET_CONFIG_VALUE_SCRIPT" -c "$UTILS_DIR/../coriolis_ansible/group_vars/all.yml" -n "$NAME")
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
