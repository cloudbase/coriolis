from oslo_service import backend

# Coriolis uses native threads. oslo.service defaults to eventlet unless told
# otherwise, and eventlet is no longer installed.
backend.init_backend(backend.BackendType.THREADING)
