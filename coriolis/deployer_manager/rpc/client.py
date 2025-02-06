# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_config import cfg
import oslo_messaging as messaging

from coriolis import constants
from coriolis import rpc

VERSION = "1.0"

deployer_manager_opts = [
    cfg.IntOpt(
        'deployer_manager_rpc_timeout',
        help="Number of seconds until RPC calls to the deployer manager "
             "timeout.")]
CONF = cfg.CONF
CONF.register_opts(deployer_manager_opts, 'deployer_manager')


class DeployerManagerClient(rpc.BaseRPCClient):

    def __init__(
            self, timeout=None):
        target = messaging.Target(
            topic=constants.DEPLOYER_MANAGER_MAIN_MESSAGING_TOPIC,
            version=VERSION)
        if timeout is None:
            timeout = CONF.deployer_manager.deployer_manager_rpc_timeout
        super(DeployerManagerClient, self).__init__(target, timeout=timeout)

    def execute_auto_deployment(
            self, ctxt, transfer_id, deployer_id, **kwargs):
        self._cast(
            ctxt, 'execute_auto_deployment', transfer_id=transfer_id,
            deployer_id=deployer_id, **kwargs)
