#!/usr/bin/env python3

import argparse
import os

import yaml


def main():
    parser = argparse.ArgumentParser(
        description="Gets or sets a Coriolis component's Docker image tag "
                     "override in a 'docker-images-config.yml'-style file. "
                     "Unlike set_config_value.py, the value is always "
                     "treated as a plain string instead of being parsed as "
                     "YAML, since Docker image tags may legitimately "
                     "contain values (such as unresolved "
                     "'{{ default_coriolis_docker_images_tag }}' Jinja "
                     "expressions) which are not valid YAML on their own.")
    parser.add_argument(
        '-c', '--config', type=str, required=True,
        help='Path to the config yml file')
    parser.add_argument(
        '-n', '--name', type=str, required=True,
        help='Name of the Docker image tag variable')
    parser.add_argument(
        '-v', '--value', type=str, default=None,
        help='If given, sets NAME to this value. Otherwise, prints the '
             'current value of NAME.')

    args = parser.parse_args()
    config_file = os.path.expanduser(args.config)

    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f.read()) or {}
    else:
        config = {}

    if args.value is None:
        print(config.get(args.name) or '')
        return

    config[args.name] = args.value

    with open(config_file, 'w') as f:
        f.write(yaml.safe_dump(config, default_flow_style=False))


if __name__ == '__main__':
    main()
