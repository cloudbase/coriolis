# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import utils as view_utils


def _format_minion_pool(minion_pool, keys=None):
    minion_pool_dict = view_utils.format_opt(minion_pool, keys)

    def _hide_minion_creds(minion_conn):
        if not minion_conn:
            return minion_conn
        if minion_conn.get('pkey'):
            minion_conn['pkey'] = '***'
        if minion_conn.get('password'):
            minion_conn['password'] = '***'
        if minion_conn.get('certificates'):
            for key in minion_conn['certificates']:
                minion_conn['certificates'][key] = '***'
    if 'minion_machines' in minion_pool_dict:
        for machine in minion_pool_dict['minion_machines']:
            if 'connection_info' in machine:
                _hide_minion_creds(machine['connection_info'])
            if 'backup_writer_connection_info' in machine:
                if machine.get('backup_writer_connection_info') and (
                        'connection_details' in machine[
                            'backup_writer_connection_info']):
                    _hide_minion_creds(
                        machine['backup_writer_connection_info'][
                            'connection_details'])

    return minion_pool_dict


def single(minion_pool, keys=None):
    return {"minion_pool": _format_minion_pool(minion_pool, keys)}


def collection(minion_pools, keys=None):
    formatted_minion_pools = [
        _format_minion_pool(r, keys) for r in minion_pools]
    return {'minion_pools': formatted_minion_pools}
