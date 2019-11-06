#!/usr/bin/env python3

import argparse
import yaml


def update_config_file(config_file):
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f.read())

    with open(("%s.sample" % config_file), 'r') as f:
        sample_config = yaml.safe_load(f.read())

    for key in sample_config.keys():
        if key in config.keys():
            continue
        config[key] = sample_config[key]

    with open(config_file, 'w') as f:
        f.write(yaml.safe_dump(config, default_flow_style=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", type=str, required=True,
                        help="The path to the config YAML file")
    args = parser.parse_args()
    update_config_file(args.config_file)
