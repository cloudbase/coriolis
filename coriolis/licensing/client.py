# Copyright 2019 Cloudbase Solutions Srl
# All Rights Reserved.

import json
import os

import requests

from coriolis import exception
from coriolis import utils
from oslo_config import cfg
from oslo_log import log as logging


LOG = logging.getLogger(__name__)
CONF = cfg.CONF

RESERVATION_TYPE_REPLICA = "replica"
RESERVATION_TYPE_MIGRATION = "migration"


class LicensingClient(object):
    """ Class for accessing the Coriolis licensing server API. """

    def __init__(self, base_url, appliance_id=None, allow_untrusted=False):
        """ :param base_url: URL for the API service, including scheme """
        self._base_url = base_url.rstrip('/')
        self._verify = not allow_untrusted
        self._appliance_id = appliance_id

    @classmethod
    def from_env(cls):
        """ Retuns a `LicensingClient` object instatiated using the
        following env vars:
        LICENSING_SERVER_BASE_URL="https://10.7.2.3:37667/v1"
        LICENSING_SERVER_ALLOW_UNTRUSTED="<set to anything>"
        LICENSING_SERVER_APP_ID_FILE="<path to licensing server ID file>"
        Returns None if 'LICENSING_SERVER_BASE_URL' is not defined.
        """
        base_url = os.environ.get("LICENSING_SERVER_BASE_URL")
        if base_url in ["", None, "None", "null"]:
            LOG.warn(
                "No 'LICENSING_SERVER_BASE_URL' env var present. Cannot "
                "instantiate licensing client.")
            return None
        allow_untrusted = os.environ.get(
            "LICENSING_SERVER_ALLOW_UNTRUSTED", False)
        client = cls(
            base_url, appliance_id=None, allow_untrusted=allow_untrusted)
        appliance_ids = client.get_appliances()
        if not appliance_ids:
            client._appliance_id = client.create_appliance().get("id")
        elif len(appliance_ids) == 1:
            client._appliance_id = appliance_ids[0].get('id')
        else:
            raise exception.CoriolisException(
                'More than one appliance IDs found.')
        client.get_licence_status()

        return client

    def _get_url_for_resource(self, resource):
        """ Provides full URL for subresource.
        Ex: "licences" -> "http://$host:$port/v1/licences"
        """
        return "%s/%s" % (self._base_url, resource.strip('/'))

    def _get_url_for_appliance_resource(self, resource):
        return "%s/appliances/%s/%s" % (
            self._base_url, self._appliance_id, resource.strip('/'))

    @utils.retry_on_error()
    def _do_req(
            self, method_name, resource, body=None,
            response_key=None, raw_response=False,
            appliance_scoped=True):
        method = getattr(requests, method_name.lower(), None)
        if not method:
            raise ValueError("No such HTTP method '%s'" % method_name)

        url = self._get_url_for_resource(resource)
        if appliance_scoped:
            url = self._get_url_for_appliance_resource(resource)

        kwargs = {"verify": self._verify,
                  "timeout": CONF.default_requests_timeout}
        if body:
            if not isinstance(body, (str, bytes)):
                body = json.dumps(body)
            kwargs["data"] = body

        LOG.debug(
            "Making '%s' call to licensing server at '%s' with body: %s",
            method_name, url, kwargs.get('data'))
        resp = method(url, **kwargs)

        if raw_response:
            return resp

        if not resp.ok:
            # try to extract error from licensing server:
            error = None
            try:
                error = resp.json().get('error', {})
            except (Exception, KeyboardInterrupt):
                LOG.debug(
                    "Exception occured during error extraction from licensing "
                    "response: '%s'\nException:\n%s",
                    resp.text, utils.get_exception_details())
            if error and all([x in error for x in ['code', 'message']]):
                raise exception.Conflict(
                    message=error['message'],
                    code=int(error['code']))
            else:
                resp.raise_for_status()

        resp_data = resp.json()
        if response_key:
            if response_key not in resp_data:
                raise ValueError(
                    "No response key '%s' in response body: %s" % (
                        response_key, resp_data))
            resp_data = resp_data[response_key]

        return resp_data

    def _get(self, resource, response_key=None, appliance_scoped=True):
        return self._do_req("GET", resource, response_key=response_key,
                            appliance_scoped=appliance_scoped)

    def _post(self, resource, body, response_key=None, appliance_scoped=True):
        return self._do_req(
            "POST", resource, body=body,
            response_key=response_key,
            appliance_scoped=appliance_scoped)

    def _put(self, resource, body, response_key=None, appliance_scoped=True):
        return self._do_req(
            "PUT", resource, body=body,
            response_key=response_key,
            appliance_scoped=appliance_scoped)

    def _delete(self, resource, body, response_key=None, appliance_scoped=True):
        return self._do_req(
            "DELETE", resource, body=body,
            response_key=response_key,
            appliance_scoped=appliance_scoped)

    def get_appliances(self):
        """ Lists all appliances on the Licensing server. """
        return self._get(
            "/appliances",
            response_key="appliances",
            appliance_scoped=False)

    def get_appliance(self):
        """ Get the appliance corresponding to this client instance. """
        return self._get(
            "/appliances/%s" % self._appliance_id, response_key="appliance",
            appliance_scoped=False)

    def create_appliance(self):
        return self._post(
            "/appliances", body=None, response_key="appliance",
            appliance_scoped=False)

    def get_licence_status(self):
        """ Gets licence status for appliance. """
        return self._get("/status", "appliance_licence_status")

    def get_licences(self):
        """ Lists all installed licences. """
        return self._get("/licences", response_key="licences")

    def add_licence(self, licence_data):
        """ Sends request to add licence (in .PEM format). """
        return self._post("/licences", licence_data)

    def add_reservation(self, reservation_type, num_vms):
        """ Creates a reservation of the given type. """
        allowed_values = [
            RESERVATION_TYPE_MIGRATION, RESERVATION_TYPE_REPLICA]
        if reservation_type not in allowed_values:
            raise ValueError(
                "Reservation type must be one of %s" % allowed_values)
        return self._post(
            "/reservations", {
                "type": reservation_type, "count": num_vms},
            response_key="reservation")

    def add_migrations_reservation(self, num_vms):
        """ Creates a reservation for the given number of VM Migrations. """
        return self.add_reservation(RESERVATION_TYPE_MIGRATION, num_vms)

    def add_replicas_reservation(self, num_vms):
        """ Creates a reservation for the given number of VM Replicas. """
        return self.add_reservation(RESERVATION_TYPE_REPLICA, num_vms)

    def get_reservations(self):
        """ Lists all existing reservations. """
        return self._get("/reservations", response_key="reservations")

    def get_reservation(self, reservation_id):
        """ Gets a reservation with the given ID.  """
        return self._get(
            "/reservations/%s" % reservation_id, response_key="reservation")

    def check_refresh_reservation(self, reservation_id):
        """ Checks the reservation with the given ID.  """
        return self._post(
            "/reservations/%s/refresh" % reservation_id, None,
            response_key="reservation")

    def delete_reservation(self, reservation_id, raise_on_404=False):
        """ Deletes a reservation by its ID.
        Unless `raise_on_404` is set, ignores not found reservations.
        """
        resp = self._do_req(
            "delete", "/reservations/%s" % reservation_id, raw_response=True)
        if not resp.ok:
            if resp.status_code == 404:
                if raise_on_404:
                    resp.raise_for_status()
                LOG.warn(
                    "Got 404 when deleting reservation '%s'", reservation_id)
            else:
                resp.raise_for_status()
