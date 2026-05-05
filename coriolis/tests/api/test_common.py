# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

import webob

from coriolis.api import common
from coriolis.tests import test_base


class ApiCommonTestCase(test_base.CoriolisBaseTestCase):
    def test_get_paging_params(self):
        req = webob.Request.blank('/some-path?marker=fake-marker&limit=10')

        marker, limit = common.get_paging_params(req)

        self.assertEqual("fake-marker", marker)
        self.assertEqual(10, limit)

    def test_get_paging_params_unspecified(self):
        req = webob.Request.blank('/some-path')

        marker, limit = common.get_paging_params(req)

        self.assertIsNone(marker)
        self.assertIsNone(limit)

    def test_get_sort_params(self):
        req = webob.Request.blank(
            '/some-path?'
            'sort_key=key0&sort_dir=dir0&sort_key=key1&sort_dir=dir1')

        sort_keys, sort_dirs = common.get_sort_params(req)

        self.assertEqual(["key0", "key1"], sort_keys)
        self.assertEqual(["dir0", "dir1"], sort_dirs)

    def test_get_sort_params_unspecified(self):
        req = webob.Request.blank('/some-path?')

        sort_keys, sort_dirs = common.get_sort_params(req)

        self.assertEqual(["created_at", "id"], sort_keys)
        self.assertEqual(["desc", "desc"], sort_dirs)
