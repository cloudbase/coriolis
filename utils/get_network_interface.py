#!/usr/bin/env python3

import argparse
import netifaces


def get_network_interface(ip):
    ifaces = netifaces.interfaces()
    for i in ifaces:
        addrs = netifaces.ifaddresses(i)[netifaces.AF_INET]
        for addr in addrs:
            if addr.get("addr") == ip:
                return i
    raise Exception("Failed to find an interface with the configured "
                    "IP addresss: %s" % ip)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", type=str, required=True,
                        help="The IP address used to find the interface")
    args = parser.parse_args()
    print(get_network_interface(args.address))
