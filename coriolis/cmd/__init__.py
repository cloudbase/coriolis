# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_service import backend

from coriolis import conf

conf.init_common_opts()

backend.init_backend(backend.BackendType.THREADING)
