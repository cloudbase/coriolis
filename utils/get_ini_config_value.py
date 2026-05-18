#!/usr/bin/env python3

import argparse
import os

from configparser import ConfigParser


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config', type=str, required=True,
        help='Path to the config ini file')
    parser.add_argument(
        '-s', '--section', type=str, required=True,
        help='Section')
    parser.add_argument(
        '-n', '--name', type=str, required=True,
        help='Name')

    args = parser.parse_args()
    config_file = os.path.expanduser(args.config)

    if not os.path.exists(config_file):
        raise Exception("Path not found: %s" % config_file)

    ini_parser = ConfigParser()
    ini_parser.read(config_file)

    if not ini_parser.has_section(args.section):
        print('')
        return

    print(ini_parser.get(args.section, args.name, raw=True, fallback=''))


if __name__ == '__main__':
    main()
