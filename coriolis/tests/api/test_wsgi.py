# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

import webob.exc

from coriolis.api import wsgi
from coriolis import exception
from coriolis.tests import test_base


class ResourceExceptionHandlerTestCase(test_base.CoriolisBaseTestCase):
    """Tests for ResourceExceptionHandler context manager."""

    def _run(self, exc_to_raise):
        """Enter the context manager and raise the given exception"""
        with wsgi.ResourceExceptionHandler():
            raise exc_to_raise

    def test_no_exception(self):
        with wsgi.ResourceExceptionHandler():
            pass  # no exception raised

    def test_not_authorized(self):
        exc = exception.NotAuthorized()
        raised = self.assertRaises(wsgi.Fault, self._run, exc)
        self.assertEqual(403, raised.status_int)

    def test_invalid(self):
        exc = exception.Invalid("bad value")
        raised = self.assertRaises(wsgi.Fault, self._run, exc)
        self.assertEqual(exc.code, raised.status_int)

    def test_type_error(self):
        exc = TypeError("wrong type")
        raised = self.assertRaises(wsgi.Fault, self._run, exc)
        self.assertEqual(400, raised.status_int)

    def test_fault(self):
        original = wsgi.Fault(webob.exc.HTTPBadRequest())
        raised = self.assertRaises(wsgi.Fault, self._run, original)
        self.assertIs(original, raised)

    def test_http_no_content(self):
        exc = webob.exc.HTTPNoContent()

        # HTTPNoContent (204) must propagate without Fault wrapping, so that
        # the HTTP client sees an empty body.
        # Fixes BadStatusLine when urllib3 reuses the connection after a
        # 204+body response.
        raised = self.assertRaises(webob.exc.HTTPNoContent, self._run, exc)
        self.assertIs(exc, raised)

    def test_http_not_found(self):
        exc = webob.exc.HTTPNotFound()
        raised = self.assertRaises(wsgi.Fault, self._run, exc)
        self.assertEqual(404, raised.status_int)

    def test_internal_server_error(self):
        exc = webob.exc.HTTPInternalServerError()
        raised = self.assertRaises(wsgi.Fault, self._run, exc)
        self.assertEqual(500, raised.status_int)

    def test_unknown_exception(self):
        exc = RuntimeError("unexpected")
        raised = self.assertRaises(RuntimeError, self._run, exc)
        self.assertIs(exc, raised)
