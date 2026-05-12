#!/bin/bash

# Coriolis LUKS firstboot cleanup for initramfs-tools (Debian / Ubuntu).
# Runs once on first boot to re-enroll TPM2, remove migration keyslots, and
# rebuild the initramfs so the embedded keyfile is gone.

set -e
set -x
KEYFILE_DIR=/etc/luks

# helpers

load_keyfile_map() {
    [ -f /etc/crypttab ] || return 0

    # Format: <target name> <source device> <key file> <options>
    while read -r _name dev_spec keyfile _; do
	# handle only coriolis migration keys.
        [[ "$keyfile" == "$KEYFILE_DIR/coriolis_"*.key ]] || continue

        local dev
        if [[ "$dev_spec" == UUID=* ]]; then
            dev=$(blkid -l -t "$dev_spec" -o device 2>/dev/null)
        else
            dev="$dev_spec"
        fi

	# Add dev: keyfile mapping, if we have a dev.
        [ -n "$dev" ] && dev_to_keyfile["$dev"]="$keyfile"
    done < <(grep -Ev '^\s*(#|$)' /etc/crypttab)
}

wait_for_tpm2() {
    for dev in /dev/tpmrm0 /dev/tpm0; do
        local deadline=$(( $(date +%s) + 10 ))
        until [ -e "$dev" ] || [ "$(date +%s)" -ge "$deadline" ]; do
	    sleep 1
        done

        if [ -e "$dev" ]; then
	    echo "$dev"
	    return
	fi
    done
}

enroll_clevis() {
    local tpm2_dev

    tpm2_dev=$(wait_for_tpm2)
    if [ -z "$tpm2_dev" ]; then
        echo "ERROR: no TPM2 device found; aborting to avoid lockout." >&2
        return 1
    fi

    echo "TPM2 device detected ($tpm2_dev); enrolling via clevis."

    for dev in "${!dev_to_keyfile[@]}"; do
        local keyfile="${dev_to_keyfile[$dev]}"

	if ! clevis luks bind -k "$keyfile" -d "$dev" tpm2 '{"pcr_ids":""}'; then
            echo "ERROR: clevis luks bind failed for $dev; aborting to avoid lockout." >&2
            return 1
        fi

	if ! cryptsetup luksDump "$dev" 2>/dev/null | grep -q 'clevis'; then
            echo "ERROR: clevis token not found in LUKS header for $dev; aborting to avoid lockout." >&2
            return 1
        fi

	echo "clevis TPM2 enrollment verified for $dev."
    done
}

remove_migration_keyslots() {
    for dev in "${!dev_to_keyfile[@]}"; do
        local keyfile="${dev_to_keyfile[$dev]}"

	echo "Removing migration keyslot from $dev using $keyfile."
        if ! cryptsetup luksRemoveKey "$dev" "$keyfile"; then
            echo "ERROR: failed to remove migration keyslot from $dev." >&2
            return 1
        fi

	echo "Migration keyslot removed from $dev."

	sed -i "s|$keyfile|none|g" /etc/crypttab
    done
}

deregister_service() {
    # Only disable, do NOT delete the unit file, or daemon-reload while the
    # service is still running. systemd would detect "Current command vanished"
    # and kill this process immediately.
    systemctl disable coriolis-luks-firstboot.service 2>/dev/null || true
}

# main

shopt -s nullglob
keyfiles=("$KEYFILE_DIR"/coriolis_*.key)
shopt -u nullglob

if [ "${#keyfiles[@]}" -eq 0 ]; then
    echo "ERROR: no migration keyfiles found in $KEYFILE_DIR; setup is broken." >&2
    exit 1
fi

echo "Found ${#keyfiles[@]} migration keyfile(s): ${keyfiles[*]}"

declare -A dev_to_keyfile
load_keyfile_map

if [ "${#dev_to_keyfile[@]}" -eq 0 ]; then
    echo "ERROR: no coriolis migration entries found in /etc/crypttab; setup is broken." >&2
    exit 1
fi

echo "Found ${#dev_to_keyfile[@]} crypttab entry / entries to process."

for _cmd in clevis clevis-encrypt-tpm2 clevis-luks-bind; do
    if ! command -v "$_cmd" >/dev/null 2>&1; then
        echo "ERROR: $_cmd not found; TPM2 enrollment requires clevis, clevis-tpm2, and clevis-luks packages." >&2
        exit 1
    fi
done

enroll_clevis

remove_migration_keyslots

echo "Deleting migration keyfiles."
rm -f "${keyfiles[@]}"
rmdir "$KEYFILE_DIR" 2>/dev/null || true

echo "Rebuilding initramfs."
# Suppress needrestart: it reboots the VM after initramfs rebuild, which doesn't
# allow us to continue with the rest of the script.
NEEDRESTART_SUSPEND=1 DEBIAN_FRONTEND=noninteractive update-initramfs -u -k all

deregister_service

echo "Firstboot LUKS cleanup complete."
rm -f "$0"

echo "Rebooting to finish LUKS first boot cleanup."
reboot
