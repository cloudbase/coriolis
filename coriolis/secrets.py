# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import json

from barbicanclient import client as barbican_client

from coriolis import keystone
from coriolis import utils


def get_secret(ctxt, secret_ref):
    session = keystone.create_keystone_session(ctxt)
    barbican = barbican_client.Client(session=session)
    sec = utils.retry_on_error()(barbican.secrets.get)(secret_ref)
    # NOTE: accessing `payload` leads to another API call being made:
    payload = utils.retry_on_error()(getattr)(sec, "payload")
    return json.loads(payload)
