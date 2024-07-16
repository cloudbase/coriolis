#!/usr/bin/env python3

import argparse
import os

import yaml


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config', type=str, required=True,
        help='Path to the config yml file')
    parser.add_argument(
        '-n', '--name', type=str, required=True,
        help='Name')
    parser.add_argument(
        '-v', '--value', type=str, required=True,
        help='Value')

    args = parser.parse_args()
    config_file = os.path.expanduser(args.config)

    name = args.name
    value = args.value

    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f.read())
    else:
        config = {}

    config[name] = yaml.safe_load(value)

    with open(config_file, 'w') as f:
        f.write(yaml.dump(config, default_flow_style=False))


if __name__ == '__main__':
    main()
