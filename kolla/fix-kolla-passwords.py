#!/usr/bin/env python3

import yaml


PASSWORDS_FILE = "/etc/kolla/passwords.yml"


def main():
    with open(PASSWORDS_FILE, "r") as f:
        passwords = yaml.safe_load(f.read())

    key = "barbican_crypto_key"
    if type(passwords[key]) is bytes:
        passwords[key] = passwords[key].decode()

    with open(PASSWORDS_FILE, 'w') as f:
        f.write(yaml.safe_dump(passwords, default_flow_style=False))


if __name__ == "__main__":
    main()
