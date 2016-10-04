# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import sys

from oslo_config import cfg
from oslo_log import log as logging
from oslo_versionedobjects import exception as obj_exc
import six
import webob.exc
from webob.util import status_generic_reasons
from webob.util import status_reasons

from coriolis.i18n import _, _LE


LOG = logging.getLogger(__name__)

CONF = cfg.CONF


class ConvertedException(webob.exc.WSGIHTTPException):

    def __init__(self, code=500, title="", explanation=""):
        self.code = code
        # There is a strict rule about constructing status line for HTTP:
        # '...Status-Line, consisting of the protocol version followed by a
        # numeric status code and its associated textual phrase, with each
        # element separated by SP characters'
        # (http://www.faqs.org/rfcs/rfc2616.html)
        # 'code' and 'title' can not be empty because they correspond
        # to numeric status code and its associated text
        if title:
            self.title = title
        else:
            try:
                self.title = status_reasons[self.code]
            except KeyError:
                generic_code = self.code // 100
                self.title = status_generic_reasons[generic_code]
        self.explanation = explanation
        super(ConvertedException, self).__init__()


class Error(Exception):
    pass


class CoriolisException(Exception):
    """Base Coriolis Exception

    To correctly use this class, inherit from it and define
    a 'message' property. That message will get printf'd
    with the keyword arguments provided to the constructor.

    """
    message = _("An unknown exception occurred.")
    code = 500
    headers = {}
    safe = False

    def __init__(self, message=None, **kwargs):
        self.kwargs = kwargs

        if 'code' not in self.kwargs:
            try:
                self.kwargs['code'] = self.code
            except AttributeError:
                pass

        for k, v in self.kwargs.items():
            if isinstance(v, Exception):
                self.kwargs[k] = six.text_type(v)

        if self._should_format(message):
            try:
                message = self.message % kwargs

            except Exception:
                exc_info = sys.exc_info()
                # kwargs doesn't match a variable in the message
                # log the issue and the kwargs
                LOG.exception(_LE('Exception in string format operation'))
                for name, value in kwargs.items():
                    LOG.error(_LE("%(name)s: %(value)s"),
                              {'name': name, 'value': value})
                if CONF.fatal_exception_format_errors:
                    six.reraise(*exc_info)
                # at least get the core message out if something happened
                message = self.message
        elif isinstance(message, Exception):
            message = six.text_type(message)

        # NOTE(luisg): We put the actual message in 'msg' so that we can access
        # it, because if we try to access the message via 'message' it will be
        # overshadowed by the class' message attribute
        self.msg = message
        super(CoriolisException, self).__init__(message)

    def _should_format(self, message):
        return message is None or '%(message)' in self.message

    def __unicode__(self):
        return six.text_type(self.msg)


class NotAuthorized(CoriolisException):
    message = _("Not authorized.")
    code = 403


class AdminRequired(NotAuthorized):
    message = _("User does not have admin privileges")


class PolicyNotAuthorized(NotAuthorized):
    message = _("Policy doesn't allow %(action)s to be performed.")


class Invalid(CoriolisException):
    message = _("Unacceptable parameters.")
    code = 400


class InvalidResults(Invalid):
    message = _("The results are invalid.")


class InvalidInput(Invalid):
    message = _("Invalid input received: %(reason)s")


class InvalidContentType(Invalid):
    message = _("Invalid content type %(content_type)s.")


class InvalidHost(Invalid):
    message = _("Invalid host: %(reason)s")


class SameDestination(Invalid):
    message = _("Origin and destination cannot be the same")


# Cannot be templated as the error syntax varies.
# msg needs to be constructed when raised.
class InvalidParameterValue(Invalid):
    message = _("%(err)s")


class InvalidAuthKey(Invalid):
    message = _("Invalid auth key: %(reason)s")


class InvalidConfigurationValue(Invalid):
    message = _('Value "%(value)s" is not valid for '
                'configuration option "%(option)s"')


class InvalidActionTasksExecutionState(Invalid):
    message = _("Invalid tasks execution state: %(reason)s")


class InvalidMigrationState(Invalid):
    message = _("Invalid migration state: %(reason)s")


class InvalidReplicaState(Invalid):
    message = _("Invalid replica state: %(reason)s")


class ServiceUnavailable(Invalid):
    message = _("Service is unavailable at this time.")


class APIException(CoriolisException):
    message = _("Error while requesting %(service)s API.")

    def __init__(self, message=None, **kwargs):
        if 'service' not in kwargs:
            kwargs['service'] = 'unknown'
        super(APIException, self).__init__(message, **kwargs)


class APITimeout(APIException):
    message = _("Timeout while requesting %(service)s API.")


class NotFound(CoriolisException):
    message = _("Resource could not be found.")
    code = 404
    safe = True


class FileNotFound(NotFound):
    message = _("File %(file_path)s could not be found.")


class InstanceNotFound(NotFound):
    message = _("Instance \"%(instance_name)s\" could not be found.")


class NetworkNotFound(NotFound):
    message = _("Network \"%(network_name)s\" could not be found.")


class Duplicate(CoriolisException):
    pass


class MalformedRequestBody(CoriolisException):
    message = _("Malformed message body: %(reason)s")


class ConfigNotFound(NotFound):
    message = _("Could not find config at %(path)s")


class ParameterNotFound(NotFound):
    message = _("Could not find parameter %(param)s")


class PasteAppNotFound(NotFound):
    message = _("Could not load paste app '%(name)s' from %(path)s")


class NoValidHost(CoriolisException):
    message = _("No valid host was found. %(reason)s")


UnsupportedObjectError = obj_exc.UnsupportedObjectError
OrphanedObjectError = obj_exc.OrphanedObjectError
IncompatibleObjectVersion = obj_exc.IncompatibleObjectVersion
ReadOnlyFieldError = obj_exc.ReadOnlyFieldError
ObjectActionError = obj_exc.ObjectActionError
ObjectFieldInvalid = obj_exc.ObjectFieldInvalid


class NotSupportedOperation(Invalid):
    message = _("Operation not supported: %(operation)s.")
    code = 405


class TaskProcessException(CoriolisException):
    pass


class OperatingSystemNotFound(NotFound):
    pass
