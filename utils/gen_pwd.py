#!/usr/bin/env python3

import argparse
import random
import string
import yaml


def genpwd(passwords_file, length):
    with open(passwords_file, 'r') as f:
        passwords = yaml.safe_load(f.read())

    for k, v in passwords.items():
        if v is not None:
            continue
        passwords[k] = ''.join([
            random.SystemRandom().choice(
                string.ascii_letters + string.digits)
            for n in range(length)
        ])

    with open(passwords_file, 'w') as f:
        f.write(yaml.safe_dump(passwords, default_flow_style=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--passwords-file", type=str, required=True,
                        help="The path to the passwords YAML file")
    parser.add_argument("--password-length", type=int, default=40,
                        help="The path to the passwords YAML file")
    args = parser.parse_args()
    genpwd(args.passwords_file, args.password_length)
