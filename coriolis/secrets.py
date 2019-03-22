# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import json

from barbicanclient import client as barbican_client

from coriolis import keystone


def get_secret(ctxt, secret_ref):
    session = keystone.create_keystone_session(ctxt)
    barbican = barbican_client.Client(session=session)
    return json.loads(barbican.secrets.get(secret_ref).payload)
