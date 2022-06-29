#!/bin/bash


# Options and prompt definitions:
OPTIONS=("Show Appliance Stats" "Show UI Login Details" "Edit/Inspect Coriolis Configuration" "Edit/Inspect Network Settings" "Expose Coriolis Services Endpoints" "Restart Coriolis Services")

WELCOME_PROMPT=$(cat <<EOP
Welcome to the Coriolis Appliance Interactive User Console!

EOP
)

OPTIONS_LINES=`for o in ${!OPTIONS[@]}; do printf '    %s. %s\n' "$o" "${OPTIONS[o]}"; done`
OPTIONS_PROMPT=$(cat <<EOP
Please select one of the following options:
EOP
)

CONTAINER_RESTART_PROMPT=$(cat <<EOP
Application of the latest settings requires restarting all Coriolis services.

WARNING: restarting the Coriolis containers can lead to the hanging/failure of any
currently-running operations.\n
EOP
)

EDITING_CONTAINER_CONSOLE_PROMPT_INSPECTING=$(cat <<EOP
For example, you may peform the following general maintenance actions:

    * vim /etc/coriolis/coriolis.conf       -- edit the main Coriolis configuration
    * cat /var/log/coriolis/*               -- checking Coriolis logs

After you are done, please exit this shell using exit or Ctrl^D.\n\n
EOP
)

EDITING_CONTAINER_CONSOLE_PROMPT_NETWORKING=$(cat <<EOP
For example, you may use the following to change common networking settings:

    * ip a; ping 8.8.8.8; nslookup google.com           -- check network status
    * vim /etc/netplan/50-cloud-init.yaml               -- edit network settings
    * echo 10.101.1.44 my.platform.local >> /etc/hosts  -- add host entries

After you are done editing, please exit this shell using exit or Ctrl^D.

Note that the networking settings are only applied *after* you edit the configurations and the
Coriolis services are restarted, so in order to validate them, please use the
Edit/Inspect Coriolis Configuration option.\n\n
EOP
)

EDITING_CONTAINER_CONSOLE_PROMPT_UPGRADE=$(cat <<EOP
Please feel free to edit the Docker registry/image settings by running:
    * vim /root/coriolis-docker/docker-images-config.yml

After you are done editing, please exit this shell using exit or Ctrl^D.\n\n
EOP
)

EXPOSING_CORIOLIS_SERVICES_PROMPT=$(cat <<EOP
This will expose the Coriolis services endpoints by setting them on the main IP address of the
appliance (instead of the default 127.0.0.1), thus allowing API access to external clients.

WARNING: This operation requires stopping all Coriolis services while updating endpoint configuration,
please make sure no running executions are active.\n\n
EOP
)


# Source parent scripts:
BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/utils/common.sh"

CORIOLIS_CONSOLE_EDITOR_CONTAINER_NAME="coriolis-console-editor"
ADMIN_OPENRC_FILE="$(get_global_config_value kolla_admin_openrc_filepath)"
CORIOLIS_CONSOLE_EDITOR_EXTRA_PROPT_FILE_PATH="/opt/coriolis/extra-coriolis-editor-prompt.txt"

# Logging options:
LOGDIR=`get_global_config_value coriolis_log_dir`
CONSOLE_SCRIPT_LOG_FILE="$LOGDIR/coriolis-console-script.log"

# General constants:
LANDSCAPE_SYSINFO_FILE_PATH=/etc/update-motd.d/50-landscape-sysinfo


# Logs all given args to the $CONSOLE_SCRIPT_LOG_FILE
function log-console-message {
    echo "$@" >> "$CONSOLE_SCRIPT_LOG_FILE"
    echo >> "$CONSOLE_SCRIPT_LOG_FILE"
}

# Runs a command and logs it in the $CONSOLE_SCRIPT_LOG_FILE:
function run-logged-command {
    ERRSWP=`mktemp`
    log-console-message "### [$(date --iso-8601=seconds)] START COMMAND: \t$@"
    out=`bash -c "$@" 2> "$ERRSWP"`
    err=`cat $ERRSWP`
    rm $ERRSWP
    log-console-message "stdout:\n$out"
    log-console-message "stderr:\n$err\n"
    log-console-message "### [$(date --iso-8601=seconds)] END COMMAND: \t$@"
    echo "$out"
}

# Returns the names of all currently-defined Coriolis containers. (including currently stopped ones)
function get-coriolis-containers {
    echo "$(run-logged-command 'docker ps -a | awk "{print \$NF}" | grep "coriolis-*" | tr "\n" " "')"
}

# Returns the IP address on the main external interface:
function get-main-ip-address {
    echo "$(run-logged-command 'python3 -c "import os; os.chdir(\"/root/coriolis-docker\"); import expose_coriolis; print(expose_coriolis.get_main_ip())"')"
}

# Runs a shell in the coriolis-console-editor container.
function run-coriolis-console-editor-shell {
    EXTRA_PROMPT="$1"
    printf "$EXTRA_PROMPT" > "$CORIOLIS_CONSOLE_EDITOR_EXTRA_PROPT_FILE_PATH"
    log-console-message "### Entering $CORIOLIS_CONSOLE_EDITOR_CONTAINER_NAME container."
    docker exec -ti $CORIOLIS_CONSOLE_EDITOR_CONTAINER_NAME bash
}

# Prints docker-related stats:
function print-docker-status {
    printf "### docker ps -a\n"
    echo "$(run-logged-command 'docker ps -a')"
    printf "### docker stats\n"
    echo "$(run-logged-command 'docker stats --no-stream')"
}

# Prints status information about the appliance.
function print-status {
    echo "$(run-logged-command $LANDSCAPE_SYSINFO_FILE_PATH)"
    printf "### df -h\n"
    echo "$(run-logged-command 'df -h')"
    print-docker-status
}

# Prints UI login details:
function print-ui-details {
    IP=`get-main-ip-address`
    echo
    printf "URL = https://$IP\n"
    cat /etc/kolla/admin-openrc.sh | grep -E "(USERNAME|PASSWORD)" | sed -e 's/export OS_//g' -e 's/=/ = /g'
    echo
    echo "NOTE: the internal IP address shown above may not be directly accessible on some clouds."
}

# Restarts all Coriolis service containers. (including currently stopped ones)
function restart-coriolis-containers {
    echo "Restarting Coriolis Service containers."
    run-logged-command "docker restart $(get-coriolis-containers)"
    if [ $? -eq 0 ]; then
        echo "Successfully restarted Coriolis Service Containers."
    fi
}

# Prompts for y/n confirmation.
function prompt-for-confirmation-word {
    PROMPT="$1"
    while true; do
        read -p "$PROMPT (y/[n]): " input
        if [ "$input" = "y" ]; then
            echo "1"
            break
        fi

        if [ "$input" = "n" ] || [ ! "$input" ]; then
            echo "0"
            break
        fi
    done
}

# Asks for user confirmation and restart all Coriolis containers. (inclusing currently stopped ones)
function confirm-restart-coriolis-containers {
    printf "$CONTAINER_RESTART_PROMPT"
    echo
    CONFIRMED=`prompt-for-confirmation-word "Restart Coriolis containers now? "`
    if [ "$CONFIRMED" = "1" ]; then
        restart-coriolis-containers $@
    else
        echo
        echo '!!! Please remember to restart the Coriolis services for any new settings to take effect !!!'
    fi
}

function expose-coriolis-services {
    printf "$EXPOSING_CORIOLIS_SERVICES_PROMPT"
    echo
    CONFIRMED=`prompt-for-confirmation-word "Expose Coriolis Services now? "`
    if [ "$CONFIRMED" = "1" ]; then
        MAIN_ADDRESS=`get-main-ip-address`
        read -p "Which IP address should the Coriolis Services be exposed to? [$MAIN_ADDRESS]: " BIND_ADDRESS
        BIND_ADDRESS=${BIND_ADDRESS:-$MAIN_ADDRESS}
        python3 $BASE_DIR/expose_coriolis.py --use-address $BIND_ADDRESS
    fi
    echo "Sourcing ~/.bashrc"
    source ~/.bashrc
}

function interact {
    echo "$OPTIONS_PROMPT"
    PS3="Select option: "
    select opt in "${OPTIONS[@]}"; do
        case $opt in
                "Show Appliance Stats")
                        print-status | less
            break
                        ;;
                "Show UI Login Details")
                        print-ui-details
            break
                        ;;
                "Edit/Inspect Coriolis Configuration")
                        run-coriolis-console-editor-shell "$EDITING_CONTAINER_CONSOLE_PROMPT_INSPECTING"
                        confirm-restart-coriolis-containers
            break
                        ;;
                "Edit/Inspect Network Settings")
                        run-coriolis-console-editor-shell "$EDITING_CONTAINER_CONSOLE_PROMPT_NETWORKING"
                        netplan apply
                        confirm-restart-coriolis-containers
            break
                        ;;
                "Expose Coriolis Services Endpoints")
                        expose-coriolis-services
            break
                        ;;
                "Restart Coriolis Services")
                        confirm-restart-coriolis-containers
            break
                        ;;
                "Upgrade Coriolis Components")
                        run-coriolis-console-editor-shell "$EDITING_CONTAINER_CONSOLE_PROMPT_UPGRADE"
                        $BASE_DIR/coriolis-ansible update
            break
                        ;;
                "Open Shell")
                        /bin/bash
            break
                        ;;
                *)
                        echo "invalid option $REPLY";;
        esac
    done
}


trap interact INT

echo "$WELCOME_PROMPT"
$LANDSCAPE_SYSINFO_FILE_PATH
while true
do
    echo
    interact
done
