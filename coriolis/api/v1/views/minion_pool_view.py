# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def _format_minion_pool(req, minion_pool, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    minion_pool_dict = dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in minion_pool.items()))

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


def single(req, minion_pool):
    return {"minion_pool": _format_minion_pool(req, minion_pool)}


def collection(req, minion_pools):
    formatted_minion_pools = [
        _format_minion_pool(req, r) for r in minion_pools]
    return {'minion_pools': formatted_minion_pools}
