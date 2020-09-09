# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import copy
import json

from barbicanclient import client as barbican_client
import keystoneauth1
from oslo_log import log as logging

from coriolis import keystone
from coriolis import utils


LOG = logging.getLogger(__name__)


def _get_barbican_secret_payload(ctxt, secret_ref):
    session = keystone.create_keystone_session(ctxt)
    barbican = barbican_client.Client(session=session)
    sec = utils.retry_on_error()(barbican.secrets.get)(secret_ref)
    # NOTE: accessing `payload` leads to another API call being made:
    payload = utils.retry_on_error()(getattr)(sec, "payload")
    return payload


def get_secret(ctxt, secret_ref):
    payload = None

    try:
        payload = _get_barbican_secret_payload(ctxt, secret_ref)
    except keystoneauth1.exceptions.http.Unauthorized:
        LOG.debug(
            "Error occured while fetching secret with trust ID, retrying "
            "without. Error was: %s", utils.get_exception_details())
        ctxt = copy.deepcopy(ctxt)
        ctxt.trust_id = None
        payload = _get_barbican_secret_payload(ctxt, secret_ref)
    except Exception:
        raise

    return json.loads(payload)
