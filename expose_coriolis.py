#!/usr/bin/env python3

import argparse
import configparser
import netifaces
import pymysql
import os
import sys
import subprocess
import toml
import urllib.parse as urlparse
import yaml

ADMIN_RC = "/etc/kolla/admin-openrc.sh"
KEYSTONE_WSGI_CFG = "/etc/kolla/keystone/wsgi-keystone.conf"
KEYSTONE_CFG = "/etc/kolla/keystone/keystone.conf"
BARBICAN_CFG = "/etc/kolla/barbican-api/barbican.conf"
BARBICAN_VASAL_CFG = "/etc/kolla/barbican-api/vassals/barbican-api.ini"
BARBICAN_WORKER_CFG = "/etc/kolla/barbican-worker/barbican.conf"
BARBICAN_KEYSTONE_CFG = "/etc/kolla/barbican-keystone-listener/barbican.conf"
CORIOLIS_CFG = "/etc/coriolis/coriolis.conf"
PROXY_CFG = "/etc/coriolis/coriolis-web-vhost.conf"
LOGGER_CONF = "/etc/coriolis-logger/coriolis-logger.toml"


def set_proxy_cfg(ip_addr):
    print("Configuring web proxy")
    if os.path.isfile(PROXY_CFG) is False:
        return
    contents = open(PROXY_CFG).readlines()
    tmp = []
    variables = [
        "keystone_auth_url_v3", "barbican_endpoint_url",
        "coriolis_base_endpoint_url", "coriolis_logger_ws_url",
        "coriolis_logger_log_url",
    ]
    for line in contents:
        for v in variables:
            if ("Define %s" % v) in line:
                spl = line.split()
                parsed = urlparse.urlparse(spl[-1])
                if parsed.hostname != ip_addr:
                    line = line.replace(
                        parsed.hostname, ip_addr)
        tmp.append(line)
    with open(PROXY_CFG, 'w') as fd:
        fd.writelines(tmp)


def set_openrc(ip_addr):
    contents = open(ADMIN_RC).readlines()
    tmp = []
    for line in contents:
        if line.startswith("export OS_AUTH_URL"):
            spl = line.split("=")
            if len(spl) != 2:
                continue
            parsed = urlparse.urlparse(spl[1])
            if parsed.hostname != ip_addr:
                line = line.replace(parsed.hostname, ip_addr)
        tmp.append(line)
    with open(ADMIN_RC, 'w') as fd:
        fd.writelines(tmp)


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


def validate_ip(ip):
    ifaces = netifaces.interfaces()
    for i in ifaces:
        addrs = netifaces.ifaddresses(i)[netifaces.AF_INET]
        for addr in addrs:
            if addr.get("addr") == ip:
                return True
    return False


class Config(object):

    def __init__(self, cfg_file):
        self._cfg_file = cfg_file
        if os.path.isfile(cfg_file) is False:
            raise ValueError("Could not find config file: %s" % cfg_file)
        self._cfg = configparser.ConfigParser()
        self._cfg.read(cfg_file)

    def save(self):
        with open(self._cfg_file, 'w') as fd:
            self._cfg.write(fd)

    def get(self, section, conf):
        return self._cfg.get(section, conf)

    def set(self, section, conf, value):
        return self._cfg.set(section, conf, value)


def get_keystone_db_cfg():
    cfg = Config(KEYSTONE_CFG)
    try:
        db_uri = cfg.get("database", "connection")
    except Exception:
        raise ValueError(
                "there is no database connection info in %s" % KEYSTONE_CFG)
    parsed = urlparse.urlparse(db_uri)
    config_dict = {
        "host": parsed.hostname,
        "user": parsed.username,
        "passwd": parsed.password,
        "db": parsed.path.lstrip('/'),
    }
    if not all(config_dict.values()):
        raise ValueError("Invalid DB connection URI in %s" % KEYSTONE_CFG)
    return config_dict


def get_mysql_connection():
    cfg = get_keystone_db_cfg()
    try:
        return pymysql.connect(**cfg)
    except Exception as err:
        raise Exception("Failed to connect to database: %s" % err)


def set_keystone_endpoints(ip_addr):
    print("Configuring keystone")
    cfg = open(KEYSTONE_WSGI_CFG).readlines()
    tmp = []
    for line in cfg:
        if line.startswith("Listen "):
            spl = line.split(" ")
            if len(spl) > 1 and ":" in spl[1]:
                _, port = spl[1].rsplit(":", 1)
                spl[1] = "%s:%s" % (ip_addr, port)
            line = " ".join(spl)
        tmp.append(line)

    with open(KEYSTONE_WSGI_CFG, 'w') as fd:
        fd.writelines(tmp)

    db_conn = get_mysql_connection()
    cursor = db_conn.cursor()
    cursor.execute("select id,url from endpoint")
    numrows = cursor.rowcount
    to_change = []
    try:
        for i in range(0, numrows):
            row = cursor.fetchone()
            if not row:
                continue
            parsed = urlparse.urlparse(row[1])
            if parsed.hostname != ip_addr:
                to_change.append([parsed.hostname, ip_addr, row[0]])
        for i in to_change:
            cmd = ("update endpoint set url = replace "
                   "(url, '%s', '%s') where id='%s'") % (i[0], i[1], i[2])
            cursor.execute(cmd)
        db_conn.commit()
    finally:
        db_conn.close()


def set_section_vals(cfg, section, vals, ip_addr):
    for i in vals:
        try:
            val = cfg.get(section, i)
            parsed = urlparse.urlparse(val)
            if parsed.hostname != ip_addr:
                val = val.replace(parsed.hostname, ip_addr)
                cfg.set(section, i, val)
        except Exception as err:
            print("Could not set %s: %s" % (i, err))


def _set_barbican_endpoints(CFG, ip_addr):
    cfg = Config(CFG)
    cfg.set("DEFAULT", "bind_host", ip_addr)

    sections_and_vals = {
        "keystone_authtoken": [
            "www_authenticate_uri", "auth_url"
        ],
        "DEFAULT": [
            "host_href",
        ],
    }

    for i in sections_and_vals:
        set_section_vals(cfg, i, sections_and_vals[i], ip_addr)
    cfg.save()


def set_barbican_endpoints(ip_addr):
    print("Configuring barbican")
    cfgs = [
        BARBICAN_CFG, BARBICAN_WORKER_CFG,
        BARBICAN_KEYSTONE_CFG]
    for i in cfgs:
        _set_barbican_endpoints(i, ip_addr)

    cfg = Config(BARBICAN_VASAL_CFG)
    try:
        val = cfg.get("uwsgi", "socket")
        spl = val.rsplit(":", 1)
        if len(spl) == 2:
            val = val.replace(spl[0], ip_addr)
            cfg.set("uwsgi", "socket", val)
    except Exception:
        pass
    cfg.save()


def set_logger_ip(ip):
    cfg = toml.load(open(LOGGER_CONF))
    apiserver = cfg.get("apiserver")
    if not apiserver:
        raise Exception("Invalid logger config. Missing apiserver section")
    bind = apiserver.get("bind")
    if bind != ip:
        cfg["apiserver"]["bind"] = ip
    
    keystone = apiserver.get("keystone_auth")
    if keystone:
        url = keystone["auth_uri"]
        parsed = urlparse.urlparse(url)
        if parsed.hostname != ip:
            new = url.replace(parsed.hostname, ip)
            cfg["apiserver"]["keystone_auth"]["auth_uri"] = new
    
    with open(LOGGER_CONF, "w") as fd:
        fd.write(toml.dumps(cfg)) 


def set_coriolis_endpoints(ip_addr):
    print("Configuring coriolis")
    cfg = Config(CORIOLIS_CFG)
    sections_and_vals = {
        "keystone_authtoken": [
            "auth_uri", "auth_url"
        ],
        "trustee": [
            "auth_url",
        ],
        "keystone": [
            "auth_url",
        ],
    }
    for i in sections_and_vals:
        set_section_vals(cfg, i, sections_and_vals[i], ip_addr)
    cfg.save()


def restart_containers():
    containers = subprocess.check_output(
            ["/usr/bin/docker", "ps", "-aq"]).decode().split()
    print("Restarting containers: %s" % ", ".join(containers))
    cmd = ["/usr/bin/docker", "restart"]
    cmd.extend(containers)
    subprocess.check_call(cmd)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--use-address",
                        type=str,
                        help="override automaticIP detection, and use the "
                        "IP specified in this option")
    args = parser.parse_args()

    ip = args.use_address or get_main_ip()
    if ip is None:
        print(
            "Unable to automatically determine main "
            "IP address and no --use-address specified")
        sys.exit(1)

    if validate_ip(ip) is False:
        print("IP address %s is not configured on this system" % ip)
        sys.exit(2)

    try:
        set_keystone_endpoints(ip)
        set_barbican_endpoints(ip)
        set_coriolis_endpoints(ip)
        set_logger_ip(ip)
        set_openrc(ip)
        set_proxy_cfg(ip)
        restart_containers()
    except Exception as err:
        print("Failed to set endpoints: %s" % err)
        sys.exit(3)

    print("Done. Please run: source ~/.bashrc to enable the "
          "new configuration in your local client.")
