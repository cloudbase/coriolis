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

    args = parser.parse_args()
    config_file = os.path.expanduser(args.config)

    name = args.name

    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
                config = yaml.safe_load(f.read())
    else:
        raise Exception("Path not found: %s" % config_file)

    print(config[name])


if __name__ == '__main__':
    main()
