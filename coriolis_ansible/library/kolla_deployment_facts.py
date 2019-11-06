#!/usr/bin/env python3

import yaml

from ansible.module_utils.basic import AnsibleModule


DOCUMENTATION = '''
---
module: kolla_deployment_facts
short_description: Module for collecting Kolla deployment facts
description:
  - A module targeting at collecting Kolla deployment facts. It is used for
    gathering info to connect to an OpenStack deployed via Kolla.
options:
  globals_file:
    description:
      - The path to the Kolla globals config YAML file
    required: False
    type: str
    default: /etc/kolla/globals.yml
  passwords_file:
    description:
      - The path to the Kolla passwords YAML file
    required: False
    type: str
    default: /etc/kolla/passwords.yml
author: Ionut Balutoiu
'''

EXAMPLES = '''
- hosts: all
  tasks:
    - name: Gather Kolla deployment facts
      kolla_deployment_facts:
      register: kolla_deployment
'''


def main():
    argument_spec = dict(
        globals_file=dict(required=False,
                          type="str",
                          default="/etc/kolla/globals.yml"),
        passwords_file=dict(required=False,
                            type="str",
                            default="/etc/kolla/passwords.yml")
    )
    module = AnsibleModule(
        argument_spec=argument_spec
    )

    with open(module.params.get('passwords_file'), 'r') as f:
        passwords = yaml.safe_load(f.read())

    with open(module.params.get('globals_file'), 'r') as f:
        global_vars = yaml.safe_load(f.read())

    return_val = dict(changed=False)
    return_val["result"] = {
        "listen_address": global_vars["kolla_internal_vip_address"],
        "keystone_project_domain_name": "Default",
        "keystone_user_domain_name": "Default",
        "keystone_project_name": "admin",
        "keystone_admin_name": "admin",
        "keystone_admin_password": passwords["keystone_admin_password"],
        "keystone_protocol": "http",
        "keystone_public_port": 5000,
        "keystone_admin_port": 35357,
        "barbican_protocol": "http",
        "barbican_port": 9311,
        "db_port": 3306,
        "db_user_name": "root",
        "db_user_password": passwords["database_password"],
        "rabbitmq_port": 5672,
        "rabbitmq_user_name": "openstack",
        "rabbitmq_user_password": passwords["rabbitmq_password"],
    }

    module.exit_json(**return_val)


if __name__ == "__main__":
    main()
