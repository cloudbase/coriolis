#!/bin/bash

NETPLAN_FILE_PATH=/etc/netplan/50-coriolis.yaml
LANDSCAPE_SYSINFO_FILE_PATH=/etc/update-motd.d/50-landscape-sysinfo
CORIOLIS_CONFIG_FILE_PATH=/etc/coriolis/coriolis.conf
CORIOLIS_DEFAULT_CONFIG_FILE_PATH=/etc/coriolis/coriolis.conf.default

function restart-coriolis-containers {
    echo
    echo "Please wait for Coriolis containers to restart..."
    docker restart $(docker ps -a | awk '{print $NF}' | grep coriolis)
    if [ $? -eq 0 ]; then
        echo "Coriolis containers restarted successfully"
	echo
    fi
}

function edit-netplan {
    editor $NETPLAN_FILE_PATH
    netplan apply
    if [ $? -eq 0 ]; then
        echo "Netplan Applied"
	echo
    fi
}

function option-selection {
    PS3="Select option: "
    options=("Edit Coriolis Configuration File" "Restore Default Coriolis Configuration" "Restart Coriolis services" "Edit Netplan" "Print SysInfo" "Change Text Editor" "Open Bash")
    select opt in "${options[@]}"; do
        case $opt in
                "Edit Coriolis Configuration File")
                        editor $CORIOLIS_CONFIG_FILE_PATH
                        restart-coriolis-containers
			break
                        ;;
                "Restore Default Coriolis Configuration")
                        /bin/cp $CORIOLIS_DEFAULT_CONFIG_FILE_PATH $CORIOLIS_CONFIG_FILE_PATH
                        restart-coriolis-containers
			break
                        ;;
                "Restart Coriolis services")
                        restart-coriolis-containers
			break
                        ;;
                "Edit Netplan")
                        edit-netplan
			break
                        ;;
                "Print SysInfo")
                        $LANDSCAPE_SYSINFO_FILE_PATH
			break
                        ;;
                "Change Text Editor")
                        sudo update-alternatives --config editor
			break
                        ;;
		"Open Bash")
			/bin/bash
			break
			;;
                *) echo "invalid option $REPLY";;
        esac
    done
}

#trap option-selection INT

echo
$LANDSCAPE_SYSINFO_FILE_PATH
while true
do
    echo
    option-selection
done
