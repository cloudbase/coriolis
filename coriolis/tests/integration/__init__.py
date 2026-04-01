# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

try:
    import eventlet

    eventlet.monkey_patch()
except ImportError:
    pass
