#!/bin/bash

# Coriolis LUKS firstboot cleanup for dracut (RHEL / Fedora / SUSE).
# Runs once on first boot to re-enroll TPM2, remove migration keyslots, and
# rebuild the initramfs so the embedded keyfile is gone.

set -e
set -x
KEYFILE_DIR=/etc/luks
DRACUT_CONF=/etc/dracut.conf.d/99-coriolis-luks.conf

# helpers

load_keyfile_map() {
    [ -f /etc/crypttab ] || return 0

    # Format: <target name> <source device> <key file> <options>
    while read -r _name dev_spec keyfile _; do
        [[ "$keyfile" == "$KEYFILE_DIR/coriolis_"*.key ]] || continue

        local dev
        if [[ "$dev_spec" == UUID=* ]]; then
            dev=$(blkid -l -t "$dev_spec" -o device 2>/dev/null)
        else
            dev="$dev_spec"
        fi

        [ -n "$dev" ] || continue

        # Add mappings.
        dev_to_keyfile["$dev"]="$keyfile"
        dev_to_spec["$dev"]="$dev_spec"
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

add_tpm2_crypttab_option() {
    local dev="$1"
    local dev_spec="${dev_to_spec[$dev]}"

    # Append ",tpm2-device=auto" to the options field of the matching entry;
    # all other lines are unchanged.
    awk -v spec="$dev_spec" '
        NF >= 4 && $2 == spec && $4 !~ /tpm2-device/ { $4 = $4 ",tpm2-device=auto" }
        { print }
    ' OFS='\t' /etc/crypttab > /tmp/.coriolis.crypttab.new
    mv /tmp/.coriolis.crypttab.new /etc/crypttab

    echo "Added tpm2-device=auto to crypttab for $dev."
}

enroll_systemd_cryptenroll() {
    local tpm2_dev

    tpm2_dev=$(wait_for_tpm2)
    if [ -z "$tpm2_dev" ]; then
        echo "ERROR: no TPM2 device found; aborting to avoid lockout." >&2
        return 1
    fi

    echo "TPM2 device detected ($tpm2_dev); enrolling via systemd-cryptenroll."

    for dev in "${!dev_to_keyfile[@]}"; do
        local keyfile="${dev_to_keyfile[$dev]}"

        if ! systemd-cryptenroll --tpm2-device=auto --tpm2-pcrs= \
                --unlock-key-file="$keyfile" "$dev" 2>/dev/null; then
            echo "ERROR: systemd-cryptenroll failed for $dev; aborting to avoid lockout." >&2
            return 1
        fi

        if ! cryptsetup luksDump "$dev" 2>/dev/null | grep -q 'systemd-tpm2'; then
            echo "ERROR: systemd-tpm2 token not found in LUKS header for $dev; aborting to avoid lockout." >&2
            return 1
        fi

        echo "systemd-cryptenroll TPM2 enrollment verified for $dev."
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
        [ "$tpm2_enrolled" = "1" ] && add_tpm2_crypttab_option "$dev"
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

declare -A dev_to_keyfile dev_to_spec
load_keyfile_map

if [ "${#dev_to_keyfile[@]}" -eq 0 ]; then
    echo "ERROR: no coriolis migration entries found in /etc/crypttab; setup is broken." >&2
    exit 1
fi

echo "Found ${#dev_to_keyfile[@]} crypttab entry / entries to process."

tpm2_enrolled=0
if command -v systemd-cryptenroll >/dev/null 2>&1; then
    enroll_systemd_cryptenroll
    tpm2_enrolled=1
fi

remove_migration_keyslots

echo "Deleting migration keyfiles."
rm -f "${keyfiles[@]}"
rmdir "$KEYFILE_DIR" 2>/dev/null || true

echo "Rebuilding initramfs."
# Embed the updated crypttab, so systemd-cryptsetup-generator uses the crypttab
# mapper name and tpm2-device=auto for auto-unlock. The Coriolis dracut.conf.d
# entry is deleted after this rebuild, so its install_items (TPM2 plugin + libtss2)
# are still picked up here.
dracut --force --include /etc/crypttab /etc/crypttab
rm -f "$DRACUT_CONF"

deregister_service

echo "Firstboot LUKS cleanup complete."
rm -f "$0"

echo "Rebooting to finish LUKS first boot cleanup."
reboot
