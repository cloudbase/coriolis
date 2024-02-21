# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import sys
from unittest import mock

from coriolis.cmd import db_sync
from coriolis.db import api as db_api
from coriolis.tests import test_base
from coriolis import utils


class DBSyncTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis db_sync CMD"""

    @mock.patch.object(db_api, 'db_sync')
    @mock.patch.object(db_api, 'get_engine')
    @mock.patch.object(utils, 'setup_logging')
    @mock.patch('coriolis.cmd.db_sync.CONF')
    @mock.patch.object(sys, 'argv')
    def test_main(
        self,
        mock_argv,
        mock_conf,
        mock_setup_logging,
        mock_get_engine,
        mock_db_sync
    ):
        db_sync.main()

        mock_conf.assert_called_once_with(
            mock_argv[1:], project='coriolis', version="1.0.0")
        mock_setup_logging.assert_called_once()
        mock_get_engine.assert_called_once()
        mock_db_sync.assert_called_once_with(mock_get_engine.return_value)
