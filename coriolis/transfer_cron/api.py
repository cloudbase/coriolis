# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def create(self, ctxt, replica_id, schedule, enabled,
               exp_date, shutdown_instance):
        return self._rpc_client.create_transfer_schedule(
            ctxt, replica_id, schedule, enabled, exp_date,
            shutdown_instance)

    def get_schedules(self, ctxt, replica_id, expired=True):
        return self._rpc_client.get_transfer_schedules(
            ctxt, replica_id, expired=expired)

    def get_schedule(self, ctxt, replica_id, schedule_id, expired=True):
        return self._rpc_client.get_transfer_schedule(
            ctxt, replica_id, schedule_id, expired=expired)

    def update(self, ctxt, replica_id, schedule_id, update_values):
        return self._rpc_client.update_transfer_schedule(
            ctxt, replica_id, schedule_id, update_values)

    def delete(self, ctxt, replica_id, schedule_id):
        self._rpc_client.delete_transfer_schedule(
            ctxt, replica_id, schedule_id)
