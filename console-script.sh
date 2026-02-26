#!/bin/bash


# Options and prompt definitions:
OPTIONS=("Show Appliance Stats" "Show UI Login Details" "Edit/Inspect Coriolis Configuration" "Edit/Inspect Network Settings" "Edit/Inspect Proxy Settings" "Expose Coriolis Services Endpoints" "Add Certificate to Coriolis Worker" "Restore to default Coriolis Worker certificate chain" "Change Coriolis API certificate chain" "Restore Coriolis API certificate chain" "Deploy External Worker" "Restart Coriolis Services" "Upgrade Coriolis Services")


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

PROXY_SETTINGS=$(cat <<EOP
This will set-up HTTP/HTTPS/NO PROXY environment variables in coriolis-worker docker container.

WARNING: This operation requires restarting all Coriolis containers and docker service,
please make sure no running executions are active.\n\n
EOP
)

ADD_CERTIFICATE_TO_WORKER_PROMPT=$(cat <<EOP
This option will append a PEM-encoded certificate to the worker, so it will be able to connect to certain
platforms (i.e. AzureStack).\n\n
EOP
)

RESTORE_WORKER_CERTIFICATE_CHAIN=$(cat <<EOP
This option will restore the worker certificate chain to its system default. Any added certificates through this prompt
will be removed, and might need to be re-added.\n\n
EOP
)

ADD_CERTIFICATE_TO_API_PROMPT=$(cat <<EOP
This option will change Coriolis API and UI server certificate with a custom defined one downloaded from an URL.
Appliance FQDN hostname will be changed to comply with server certificate, API's will be exposed.\n\n
EOP
)

RESTORE_API_CERTIFICATE_CHAIN=$(cat <<EOP
This option will restore the Coriolis API and UI server certificate to the internal self-signed certificate.\n\n
EOP
)

DEPLOY_EXTERNAL_WORKER_PROMPT=$(cat <<EOP
This option will deploy a Coriolis Worker service node to an external machine.
Coriolis services need to be exposed in order to facilitate inter-node communication.
If the appliance is already exposed and its API is reachable, skip exposing in this command wizard.\n\n
EOP
)

# Source parent scripts:
BASE_DIR=$(dirname "$(readlink -f "$0")")
source "$BASE_DIR/utils/common.sh" 2> /dev/null

CORIOLIS_CONSOLE_EDITOR_CONTAINER_NAME="coriolis-console-editor"
ADMIN_OPENRC_FILE="$(get_global_config_value kolla_admin_openrc_filepath)"
CORIOLIS_CONSOLE_EDITOR_EXTRA_PROPT_FILE_PATH="/opt/coriolis/extra-coriolis-editor-prompt.txt"

# Logging options:
LOGDIR=`get_global_config_value coriolis_log_dir`
CONSOLE_SCRIPT_LOG_FILE="$LOGDIR/coriolis-console-script.log"

# General constants:
LANDSCAPE_SYSINFO_FILE_PATH=/etc/update-motd.d/50-landscape-sysinfo
TMP_WORKER_CERTIFICATE_PATH=/etc/coriolis/tmp-worker-cert.pem
DOCKER_CONTAINERS_FOLDER="/var/lib/docker/containers"
CORIOLIS_CERT_FOLDER=$(get_global_config_value coriolis_certificate_store)
API_CA_PATH=$(get_global_config_value coriolis_appliance_tls_cacert)
API_CUSTOM_LOCAL_CA_PATH="/usr/local/share/ca-certificates/coriolis-custom-ca.crt"
API_CERT_PATH=$(get_global_config_value coriolis_appliance_tls_certificate)
API_KEY_PATH=$(get_global_config_value coriolis_appliance_tls_key)
METAL_ENABLED=$(get_global_config_value coriolis_export_providers | grep metal)
METAL_HUB_CERTS_PATH=$(get_global_config_value coriolis_metal_hub_certs_dir)
OLD_HOSTNAME="/etc/hostname.bak"

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
    local exit_code=$?
    err=`cat $ERRSWP`
    rm $ERRSWP
    log-console-message "stdout:\n$out"
    log-console-message "stderr:\n$err\n"
    log-console-message "### [$(date --iso-8601=seconds)] END COMMAND: \t$@"
    echo "$out"
    # Explicitly return 1 if the command failed
    if [[ $exit_code -ne 0 ]]; then
        log-console-message "ERROR: Command failed with exit code $exit_code"
        return 1
    fi
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
    FQDN_APPLIANCE_NAME=$(hostname)
    echo
    printf "URL = https://$FQDN_APPLIANCE_NAME\n"
    printf "URL = https://$IP\n"
    cat /etc/kolla/admin-openrc.sh | grep -E "(USERNAME|PASSWORD)" | sed -e 's/export OS_//g' -e 's/=/ = /g'
    echo
    echo "NOTE: the internal IP address shown above may not be directly accessible on some clouds."
}

# Restarts all Coriolis service containers. (including currently stopped ones)
function restart-coriolis-containers {
    echo "Restarting Coriolis Logger service."
    run-logged-command "systemctl restart coriolis-logger.service"
    if [ $? -eq 0 ]; then
        echo "Successfully restarted Coriolis Logger Service."
    fi
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

#Ask to confirm input
confirm_input() {
    PROMPT="$1"
    local input

    while true; do
        read -rp "$PROMPT" input
        read -rp "You entered: '$input'. Confirm? (yes/no/cancel): " choice
        case "$choice" in
            [Yy]* ) echo "$input"; return 0;;
            [Nn]* ) echo "";;
            [Cc]* ) echo "";return 1;;
            * ) echo "";;
        esac
    done
}

#Download URL to file
download_url_to_file() {
    URL="$1"
    LOCAL_FILE="$2"
    echo "Downloading file from URL: $URL to path: $LOCAL_FILE"
    run-logged-command "wget -q --no-check-certificate -O $LOCAL_FILE $URL"
    if ! [ $? -eq 0 ]; then
        echo "ERROR: Failed to download file from URL: $URL "
        return 1
    fi
}
#Generic certificate check
function check_certificate() {
    CHECK="$1"
    echo "Checking certificate file $CHECK."
    run-logged-command "openssl x509 -noout -in $CHECK"
    if ! [ $? -eq 0 ]; then
        echo "ERROR: The provided certificate does not contain a valid PEM certificate!"
        return 1
    fi
}

#Generic certificate key file check
function check_certificate_key() {
    CHECK="$1"
    echo "Checking certificate file $CHECK."
    run-logged-command "openssl pkey --check --noout --in $CHECK"
    if ! [ $? -eq 0 ]; then
        echo "ERROR: The provided certificate keys is not valid!"
        return 1
    fi
}

#Validate certificate against hostname
function validate_certificate_fqdn() {
    CRT="$1"
    NME="$2"
    CHECK=$(run-logged-command "openssl x509 -noout -in $CRT -checkhost $NME")
    if [ $? -eq 0 ]; then
        if [[ $CHECK =~ "NOT" ]]; then
            echo "ERROR: Failed to verify certificate $CRT against hostname $NME."
            return 1
        else
            return 0
        fi
    fi
}

#Backup file in same path
function backup_file {
    S_FILE="$1"
    D_FILE="$2"
    if [ -f "$S_FILE" ]; then
        if [ ! -f "$D_FILE" ]; then
            run-logged-command "cp $S_FILE $D_FILE"
        fi
    else
        echo "File: $S_FILE does not exist."
    fi
}

#Restore file in same path
function restore_file {
    S_FILE="$1"
    D_FILE="$2"
    log-console-message "Attempting restore of file $S_FILE to $D_FILE"
    if [ -f $S_FILE ]; then
        run-logged-command "cp $S_FILE $D_FILE"
    else echo "File: $S_FILE does not exist."
    fi
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

function add-certificate-to-worker {
    printf "$ADD_CERTIFICATE_TO_WORKER_PROMPT"
    CERTIFI_DIR=$(run-logged-command "docker exec coriolis-worker python3 -c 'import certifi; import os; print(os.path.dirname(certifi.__file__))'")
    CACERT_PEM_PATH="$CERTIFI_DIR/cacert.pem"

    local cert_setup=1
    while true; do
        cert=$(confirm_input "Please enter a URL containing the certificate data: ")
        if [ $? -eq 0 ]; then
            if download_url_to_file "$cert" "$TMP_WORKER_CERTIFICATE_PATH"; then
                echo "Download succeeded."
                if check_certificate "$TMP_WORKER_CERTIFICATE_PATH"; then
                    echo "Server Certificate $TMP_WORKER_CERTIFICATE_PATH is valid."
                    cert_setup=0
                    break
                fi
            fi
        else
            break
        fi
    done

    if [[ $cert_setup = 0 ]]; then
        run-logged-command "docker exec coriolis-worker bash -c 'if [ ! -f $CACERT_PEM_PATH.bak ]; then cp $CACERT_PEM_PATH $CACERT_PEM_PATH.bak; fi'"
        run-logged-command "docker exec coriolis-worker bash -c 'cat $TMP_WORKER_CERTIFICATE_PATH >> $CACERT_PEM_PATH; echo >> $CACERT_PEM_PATH'"
    fi
}

function restore-certificate-chain {
    printf "$RESTORE_WORKER_CERTIFICATE_CHAIN"
    CONFIRMED=`prompt-for-confirmation-word "Restore certificate chain now? "`
    CERTIFI_DIR=$(run-logged-command "docker exec coriolis-worker python3 -c 'import certifi; import os; print(os.path.dirname(certifi.__file__))'")
    CACERT_PEM_PATH="$CERTIFI_DIR/cacert.pem"
    if [ "$CONFIRMED" = "1" ]; then
        run-logged-command "docker exec coriolis-worker bash -c 'if [ -f $CACERT_PEM_PATH.bak ]; then cp $CACERT_PEM_PATH.bak $CACERT_PEM_PATH; fi'"
    fi
}

function display-proxy-settings {
    CURR_PROXY_SETTINGS=$(run-logged-command "docker exec -ti coriolis-worker env|grep PROXY")
    echo -e "Current Proxy settings:\n$CURR_PROXY_SETTINGS\n"
}

#checks coriolis-worker .json configuration file for env. var. existence and creates new/updates values
#$1 argument ccontains the env. variable name, $2 the value of it, $3 coriolis-worker container .json configuration file path
function apply-proxy-variable {
    VAR_PRESENT=$(run-logged-command "jq --arg value $1 '.Config.Env | contains(["'$value'"])' $2")
    if [[ $VAR_PRESENT == "true" ]]; then
        run-logged-command "jq --arg var1 $1 --arg value $1=$3 '(.Config.Env[] | select(contains("'$var1'"))) |= "'($value ? // .)'"' $2 > "$2.tmp" && mv "$2.tmp" "$2""
    else
        run-logged-command "jq --arg var1 $1 --arg value $1=$3 '(.Config.Env += ["'$value'"])' $2 > "$2.tmp" && mv "$2.tmp" "$2""
    fi
}

#127.0.0.1 and Coriolis Appliance IP@ needs to skip proxy since Coriolis internal endpoints are using it.
function add-proxy {
    printf "$PROXY_SETTINGS"
    WORKER_CONT_ID=$(run-logged-command 'docker inspect --format="{{.Id}}" coriolis-worker')
    WORKER_CONT_CONF_FILE="$DOCKER_CONTAINERS_FOLDER/$WORKER_CONT_ID/config.v2.json"
    IP_ADDR=`get-main-ip-address`

    display-proxy-settings
    CONTINUE_SETUP=`prompt-for-confirmation-word "Continue setting-up proxy environment variables? "`
    if [ "$CONTINUE_SETUP" = "0" ]; then
        echo
        return
    fi

    read -p "Enter HTTP proxy ( http://<HOST>:<PORT>, Enter=None ): " PROXY_HTTP
    read -p "Enter HTTPS proxy ( http(s)://<HOST>:<PORT>, Enter=None ): " PROXY_HTTPS
    read -p "Enter NO proxy ( *.test.example.com,.example2.com ): " PROXY_SKIP
    APPLY_PROXY_WORKER=`prompt-for-confirmation-word "Apply Proxy settings to Coriolis Worker container? "`
    if [ "$APPLY_PROXY_WORKER" = "1" ]; then
        echo
        echo 'Stopping coriolis-worker container'
        run-logged-command "docker stop coriolis-worker 2>&1 > /dev/null"
        if ! [ $? -eq 0 ]; then
            echo "ERROR: Failed to stop coriolis-worker container."
            return
        fi
        apply-proxy-variable HTTP_PROXY $WORKER_CONT_CONF_FILE $PROXY_HTTP
        apply-proxy-variable HTTPS_PROXY $WORKER_CONT_CONF_FILE $PROXY_HTTPS
        apply-proxy-variable NO_PROXY $WORKER_CONT_CONF_FILE 127.0.0.1,$IP_ADDR,$PROXY_SKIP
        echo "Restarting docker service."
        run-logged-command "systemctl restart docker"
        echo "Starting coriolis-worker container."
        run-logged-command "docker start coriolis-worker 2>&1 > /dev/null"
        if ! [ $? -eq 0 ]; then
            echo "ERROR: Failed to start coriolis-worker container."
            return
        fi
        display-proxy-settings
    else
        echo
        echo 'Proxy settings not applied to Coriolis Worker container.'
    fi
}

function change-api-certificate {
    printf "$ADD_CERTIFICATE_TO_API_PROMPT"

    local cert_setup=1
    local ca_setup=1
    local key_setup=1
    local hostname_setup=1
    echo "Creating backup files."
    for f in $(ls $CORIOLIS_CERT_FOLDER/*.pem) ; do backup_file "$f" "$f.bak" ; done
    if [ -n "$METAL_ENABLED" ]; then
        for f in $(ls $METAL_HUB_CERTS_PATH/*.pem) ; do backup_file "$f" "$f.bak" ; done
    fi
    if [ ! -f "$OLD_HOSTNAME" ]; then
        run-logged-command "echo $(hostname) > $OLD_HOSTNAME"
    fi

    while true; do
        cert=$(confirm_input "Enter URL for Server Certificate: ")
        if [ $? -eq 0 ]; then
            if download_url_to_file "$cert" "$API_CERT_PATH"; then
                echo "Download succeeded."
                if check_certificate "$API_CERT_PATH"; then
                    echo "Server Certificate $API_CERT_PATH is valid."
                    cert_setup=0
                    break
                fi
            fi
        else
            break
        fi
    done

    while true; do
        ca=$(confirm_input "Enter URL for Root CA Certificate: ")
        if [ $? -eq 0 ]; then
            if download_url_to_file "$ca" "$API_CA_PATH"; then
                echo "Download succeeded."
                if check_certificate "$API_CA_PATH"; then
                    echo "Root CA certificate $API_CA_PATH is valid."
                    ca_setup=0
                    break
                fi
            fi
        else
            break
        fi
    done

    while true; do
        cert_key=$(confirm_input "Enter URL for Certificate Key: ")
        if [ $? -eq 0 ]; then
            if download_url_to_file "$cert_key" "$API_KEY_PATH"; then
                echo "Download succeeded."
                if check_certificate_key "$API_KEY_PATH"; then
                    echo "Certificate Key $API_KEY_PATH is valid."
                    key_setup=0
                    break
                fi
            fi
        else
            break
        fi
    done

    while true; do
        FQDN_NAME=$(confirm_input "Please enter the full FQDN hostname for Coriolis Appliance for which the certificate is valid for: ")
        if [ $? -eq 0 ]; then
            echo "Validating new hostname."
            if validate_certificate_fqdn "$API_CERT_PATH" "$FQDN_NAME"; then
                echo "The provided certificate is valid for hostname $FQDN_NAME."
                run-logged-command "hostnamectl set-hostname $FQDN_NAME"
                if [ $? -eq 0 ]; then
                    echo "Hostname $FQDN_NAME is set."
                    hostname_setup=0
                    break
                else
                    echo "ERROR: The new hostname $FQDN_NAME is not valid. Please enter a valid hostname."
                    hostname_setup=1
                fi
            fi
        else
            break
        fi
    done

    if [[ $cert_setup = 1 || $ca_setup = 1 || $key_setup = 1 || $hostname_setup = 1 ]]; then
    echo "Certificate setup incomplete. Will restore backup files."
        for f in $(ls $CORIOLIS_CERT_FOLDER/*.bak); do restore_file "$f" "${f%.*}"; done
        if [ -n "$METAL_ENABLED" ]; then
            for f in $(ls $METAL_HUB_CERTS_PATH/*.bak); do restore_file "$f" "${f%.*}"; done
        fi
        if [ -f $OLD_HOSTNAME ]; then
            run-logged-command "hostnamectl set-hostname $(cat $OLD_HOSTNAME)"
        else
            echo "Keep existing hostname $(hostname) ."
        fi
    else
        echo "Creating certificate bundle files."
        run-logged-command "cat $API_CERT_PATH $API_CA_PATH > $(get_global_config_value coriolis_appliance_tls_cert_bundle)"
        run-logged-command "cat $API_KEY_PATH $API_CERT_PATH $API_CA_PATH > $(get_global_config_value coriolis_appliance_tls_combined)"

        run-logged-command "cp $API_CA_PATH $API_CUSTOM_LOCAL_CA_PATH"
        run-logged-command "chmod 644 $API_CUSTOM_LOCAL_CA_PATH"
        run-logged-command "update-ca-certificates"

        echo "Create mark file for using custom certififcates."
        run-logged-command "touch $(get_global_config_value coriolis_appliance_custom_cert)"

        if [ -n "$METAL_ENABLED" ]; then
        echo "Setting Coriolis Metal Hub Certificate chain."
            run-logged-command "cp $API_CA_PATH $(get_global_config_value coriolis_metal_hub_ca_cert_path)"
            run-logged-command "cp $API_CERT_PATH $(get_global_config_value coriolis_metal_hub_client_cert_path)"
            run-logged-command "cp $API_CERT_PATH $(get_global_config_value coriolis_metal_hub_server_cert_path)"
            run-logged-command "cp $API_KEY_PATH $(get_global_config_value coriolis_metal_hub_server_key_path)"
            run-logged-command "cp $API_KEY_PATH $(get_global_config_value coriolis_metal_hub_client_key_path)"
        fi
        CONTINUE_EXPOSE=`prompt-for-confirmation-word "New certificate setup complete. Coriolis Appliance needs to be exposed for the changes to take effect."`
        if [ "$CONTINUE_EXPOSE" = "0" ]; then
            echo "Coriolis services not exposed yet."
        return
        fi
        $SET_CONFIG_VALUE_SCRIPT -c $CONFIG_FILE -n reject_step_ca -v "true"
        expose-coriolis-services
    fi
}

function restore-certificate-chain-api {

    printf "$RESTORE_API_CERTIFICATE_CHAIN"
    CONFIRMED=`prompt-for-confirmation-word "Confirm reverting server certificate to self-signed"`
    if [ "$CONFIRMED" = "1" ]; then
        echo "Removing custom certificates setup."
        if [ -f $CORIOLIS_CERT_FOLDER/custom ]; then
            run-logged-command "rm $CORIOLIS_CERT_FOLDER/custom"
            for f in $CORIOLIS_CERT_FOLDER/*.bak ; do restore_file "$f" "${f%.*}"; done
            if [ -n "$METAL_ENABLED" ]; then
                for f in $METAL_HUB_CERTS_PATH/*.bak ; do restore_file "$f" "${f%.*}"; done
            fi
            if [ -f $API_CUSTOM_LOCAL_CA_PATH ]; then
                run-logged-command "rm $API_CUSTOM_LOCAL_CA_PATH"
                run-logged-command "update-ca-certificates --fresh"
            fi
        fi
    fi
    printf "Resetting hostname."
    if [ -f $OLD_HOSTNAME ]; then
        run-logged-command "hostnamectl set-hostname $(cat $OLD_HOSTNAME)"
    else
        echo "Keep existing hostname $(hostname) ."
    fi

    CONTINUE_EXPOSE=`prompt-for-confirmation-word "Custom certificate setup removed. Coriolis Appliance might need to be exposed for the changes to take effect  "`
    if [ "$CONTINUE_EXPOSE" = "0" ]; then
        echo "Coriolis services not exposed yet."
        return
    fi
    expose-coriolis-services
}

function deploy-external-worker {
    printf "$DEPLOY_EXTERNAL_WORKER_PROMPT"
    CONFIRMED=`prompt-for-confirmation-word "Do you need to expose Coriolis services?"`
    if [ "$CONFIRMED" = "1" ]; then
        expose-coriolis-services
    fi
    DEFAULT_USER="root"

    read -rp "Input external machine host: " EXT_HOST
    read -rp "Input SSH username [$DEFAULT_USER]: " EXT_USER
    EXT_USER=${EXT_USER:-$DEFAULT_USER}
    read -s -r -p "Input SSH password: " EXT_PASS

    echo -e "\nTesting SSH connection..."
    sshpass -p "$EXT_PASS" ssh -oStrictHostKeyChecking=accept-new $EXT_USER@$EXT_HOST exit
    if [ $? != "0" ]; then
        echo "SSH connection to external machine failed. Please make sure that connection details are correct."
        return
    fi

    EXT_PATH="$BASE_DIR/coriolis_ansible/inventory/external_workers"
    cp $EXT_PATH $EXT_PATH.bak
    echo "[external_workers]" > $EXT_PATH
    echo -e "$EXT_HOST\tansible_connection=ssh\tansible_python_interpreter=/usr/bin/python3\tansible_ssh_user=$EXT_USER\tansible_ssh_pass=$EXT_PASS" >> $EXT_PATH

    echo "Starting external worker deployment process"
    $BASE_DIR/coriolis-ansible deploy-workers && echo "External worker successfully deployed"
}

function upgrade-coriolis-services {

    pushd "$BASE_DIR"
    current_release=$(cat /etc/coriolis/coriolis.release)
    current_commit=$(git show --pretty="format:%H" --no-patch)
    coriolis_tag=$(confirm_input "Coriolis tag: ")
    if [[ $coriolis_tag == "latest"  ]];then
        echo "WARNING: Please use a Coriolis Release version!"
        return
    fi

    if (( $(echo "$coriolis_tag <= $current_release" | bc -l) ));then
        echo "ERROR: You need to upgrade to a higher Coriolis Release than the one you have."
        echo "Current Coriolis release $(cat /etc/coriolis/coriolis.release)"
        return
    fi

    REG="registry.cloudbase.it"
    docker login "$REG" || true
    docker image pull -q "$REG"/appliance/coriolis-metal-hub:"$coriolis_tag" &> /dev/null
    if [ $? -ne 0 ]; then
        echo "ERROR: That Coriolis Release does not exist!"
        return
    fi

    success=0
    DBPASS=$(grep -E '^(database_password)' /etc/kolla/passwords.yml | awk '{print $2}')
    docker exec mariadb mysqldump -u root -p"$DBPASS" --all-databases > /root/coriolis_appliance_dbs_backups.sql &&
    tar -czf /root/coriolis_etc_kolla_backup.tar.gz /etc/kolla/ &&
    tar -czf /root/coriolis_etc_coriolis_backup.tar.gz /etc/coriolis/ &&
    cd /root/coriolis-docker &&
    git fetch origin &&
    git checkout origin/stable/"${coriolis_tag%.*}" &&
    sed -i "s@^docker_pull_images.*@docker_pull_images: true@g" /root/coriolis-docker/docker-images-config.yml &&
    sed -i "s@^default_coriolis_docker_images_tag.*@default_coriolis_docker_images_tag: $coriolis_tag@g" /root/coriolis-docker/docker-images-config.yml &&
    python3 "$BASE_DIR"/upgrade_coriolis.py -u "$BASE_DIR/utils/upgrade_scripts" -c $current_release -n $coriolis_tag ||
    { success=1;}

    if [[ $success -eq 1 ]]; then {
        docker exec -i mariadb mysql -u root -p"$DBPASS" < /root/coriolis_appliance_dbs_backups.sql
        tar -xf /root/coriolis_etc_kolla_backup.tar.gz -C /
        tar -xf /root/coriolis_etc_coriolis_backup.tar.gz -C /
        git checkout $current_commit
    }
    fi

    rm /root/coriolis_appliance_dbs_backups.sql
    rm /root/coriolis_etc_kolla_backup.tar.gz
    rm /root/coriolis_etc_coriolis_backup.tar.gz
    popd
    return
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
                "Edit/Inspect Proxy Settings")
                        add-proxy
            break
                        ;;
                "Expose Coriolis Services Endpoints")
                        expose-coriolis-services
            break
                        ;;
                "Add Certificate to Coriolis Worker")
                        add-certificate-to-worker
                        confirm-restart-coriolis-containers
            break
                        ;;
                "Restore to default Coriolis Worker certificate chain")
                        restore-certificate-chain
                        confirm-restart-coriolis-containers
            break
                        ;;
                "Change Coriolis API certificate chain")
                        change-api-certificate
            break
                        ;;
                "Restore Coriolis API certificate chain")
                        restore-certificate-chain-api
            break
                        ;;
                "Deploy External Worker")
                        deploy-external-worker
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
                "Upgrade Coriolis Services")
                        upgrade-coriolis-services
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
