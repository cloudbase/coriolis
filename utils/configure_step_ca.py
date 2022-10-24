#!/usr/bin/env python3

import json
import netifaces
import os
import sys
import yaml


CONFIG_PATHS = [
    "/root/coriolis-docker/config.yml",
    "/root/coriolis-docker/coriolis_ansible/group_vars/all.yml"]


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


def get_config(config_file):
    if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f.read())
    else:
        print("Path not found: %s" % config_file)
        return

    return config


def main():
    set_dns = bool("setdnsnames" in sys.argv)
    step_ca_home = None
    for config_path in CONFIG_PATHS:
        config = get_config(config_path)
        if config:
            step_ca_home = config.get('step_ca_home')
            if step_ca_home:
                break

    if step_ca_home:
        with open("%s/config/ca.json" % step_ca_home, "r+") as f:
            data = json.load(f)
            f.seek(0)
            data['authority']['claims'] = {"maxTLSCertDuration": "87672h",
                                           "defaultTLSCertDuration": "87672h"}
            if set_dns:
                main_ip_addr = get_main_ip()
                if main_ip_addr and main_ip_addr not in data['dnsNames']:
                    data['dnsNames'].append(main_ip_addr)
            json.dump(data, f, indent=2)
    else:
        raise Exception(
            "No step_ca_home option set in files: %s" % CONFIG_PATHS)


if __name__ == "__main__":
    main()
