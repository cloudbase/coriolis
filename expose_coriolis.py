#!/usr/bin/env python3

import argparse
import netifaces
import os
import re
import socket
import sys
import subprocess
import yaml


BASE_DIR = os.path.dirname(os.path.realpath(__file__))

KOLLA_CFG = "/etc/kolla/globals.yml"
KOLLA_INVENTORY_FILE = os.path.join(BASE_DIR, "kolla/coriolis")
APPLIANCE_INVENTORY_FILE = os.path.join(BASE_DIR,
                                        "coriolis_ansible/inventory/appliance")
CORIOLIS_CFG = os.path.join(BASE_DIR, "config.yml")
CORIOLIS_ANSIBLE_BIN = os.path.join(BASE_DIR, "coriolis-ansible")


def get_main_ip():
    gateways = netifaces.gateways()
    default = gateways.get("default")
    if not default or len(default) == 0:
        return None
    _, dev = default.popitem()[1]
    devAddrs = netifaces.ifaddresses(dev)[netifaces.AF_INET]
    if not devAddrs or len(devAddrs) == 0:
        return None
    return devAddrs[0]["addr"]


def get_interface(ip):
    ifaces = netifaces.interfaces()
    for i in ifaces:
        addrs = netifaces.ifaddresses(i).get(netifaces.AF_INET, [])
        for addr in addrs:
            if addr.get("addr") == ip:
                return i
    return None


def edit_inventory_file_section(inventory_file, section, new_host_name=None,
                                 new_ansible_connection=None,
                                 new_ansible_python_interpreter=None):
    section_re = r'^\[(.*)\]$'
    separator = " " * 8
    with open(inventory_file) as fd:
        lines = fd.read().splitlines()

    host_line_idx = None
    for i, line in enumerate(lines):
        if line.startswith("#"):
            continue
        m = re.match(section_re, line.strip())
        if m and m[1] == section:
            host_line_idx = i + 1
            break

    if host_line_idx is None:
        raise Exception(f"Section {section} not found in {inventory_file}")

    host_line = lines[host_line_idx]
    host_name, ansible_connection, ansible_interpreter = host_line.split()

    if new_host_name:
        host_name = new_host_name
    if new_ansible_connection:
        conn_key, conn_value = ansible_connection.split('=')
        ansible_connection = f'{conn_key}={new_ansible_connection}'
    if new_ansible_python_interpreter:
        interpreter_key, _ = ansible_interpreter.split('=')
        ansible_interpreter = (
            f'{interpreter_key}={new_ansible_python_interpreter}')

    lines[host_line_idx] = separator.join(
        [host_name, ansible_connection, ansible_interpreter])

    with open(inventory_file, 'w') as fd:
        fd.write('\n'.join(lines))


def update_kolla_cfg(interface, ip, fqdn):
    print("Updating Kolla config")

    with open(KOLLA_CFG, "r") as f:
        config = yaml.safe_load(f)

    config["network_interface"] = interface
    config["kolla_internal_vip_address"] = ip
    if fqdn == '':
        if config["kolla_internal_fqdn"]:
            config.pop('kolla_internal_fqdn', None)
        if config["kolla_external_fqdn"]:
            config.pop('kolla_external_fqdn', None)
    else:
        config["kolla_internal_fqdn"] = fqdn
        config["kolla_external_fqdn"] = fqdn

    with open(KOLLA_CFG, 'w') as f:
        f.write(yaml.safe_dump(config, default_flow_style=False))


def update_coriolis_cfg(ip, fqdn):
    print("Updating Coriolis config")

    with open(CORIOLIS_CFG, "r") as f:
        config = yaml.safe_load(f)

    config["bind_address"] = ip
    config["coriolis_certtificate_fqdn"] = fqdn

    with open(CORIOLIS_CFG, 'w') as f:
        f.write(yaml.safe_dump(config, default_flow_style=False))


def update_appliance_inventory(fqdn):
    print("Updating Coriolis Appliance inventory file")

    edit_inventory_file_section(
        inventory_file=APPLIANCE_INVENTORY_FILE,
        section="appliance",
        new_host_name=fqdn)


def remove_container(container):
    print("Removing docker container %s" % container)
    try:
        subprocess.check_call(
            ["/usr/bin/docker", "stop", container, "-t", "30"])
        subprocess.check_call(["/usr/bin/docker", "wait", container])
        subprocess.check_call(["/usr/bin/docker", "rm", container])
    except subprocess.CalledProcessError as err:
        print(err.output)
        return

def expose():
    print("Exposing Coriolis appliance")

    remove_container("rabbitmq")

    subprocess.check_call([CORIOLIS_ANSIBLE_BIN, "bootstrap"])

    base_cmd = ["kolla-ansible", "-i", KOLLA_INVENTORY_FILE]
    subprocess.check_call(base_cmd + ["reconfigure"])
    subprocess.check_call(base_cmd + ["post-deploy"])

    subprocess.check_call([CORIOLIS_ANSIBLE_BIN, "reconfigure"])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--use-address",
                        type=str,
                        help="override automaticIP detection, and use the "
                             "IP specified in this option")
    args = parser.parse_args()

    ip = args.use_address or get_main_ip()
    if ip is None:
        print("Unable to automatically determine main "
              "IP address and no --use-address specified")
        sys.exit(1)

    interface = get_interface(ip)
    fqdn = socket.gethostname()
    if interface is None:
        print("IP address %s is not configured on this system" % ip)
        sys.exit(2)
    if interface == 'lo':
        fqdn = ''

    try:
        update_kolla_cfg(interface, ip, fqdn)
        update_coriolis_cfg(ip, fqdn)
        update_appliance_inventory(fqdn)
        expose()
    except Exception as err:
        print("Failed to expose Coriolis appliance: %s" % err)
        sys.exit(3)

    print("Done. Please run: source ~/.bashrc to enable the "
          "new configuration in your local client.")
