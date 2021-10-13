# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import abc
import collections
import copy

from oslo_log import log as logging
from six import with_metaclass

from coriolis import constants


LOG = logging.getLogger(__name__)

_PercStepData = collections.namedtuple(
    "_PercStepData", "progress_update_id last_perc last_value total_steps")


class EventManager(object, with_metaclass(abc.ABCMeta)):

    def __init__(self, event_handler):
        self._event_handler = event_handler
        self._perc_steps = {}

    def _call_event_handler(self, method_name, *args, **kwargs):
        if self._event_handler:
            method_obj = getattr(self._event_handler, str(method_name), None)
            if not method_obj:
                raise AttributeError(
                    "No method named '%s' for event handler of type '%s'." % (
                        method_name, type(self._event_handler)))
            return method_obj(*args, **kwargs)

    def add_percentage_step(self, message, total_steps, initial_step=0):
        if total_steps < 0:
            LOG.warn(
                "Max percentage value was negative (%s). Reset to 0",
                total_steps)
            total_steps = 0
        if total_steps == 0:
            LOG.warn("Max percentage value set to 0 (zero)")

        if initial_step > total_steps:
            raise ValueError(
                "Provided percent step initial value '%s' is larger than the "
                "maximum value '%s'" % (initial_step, total_steps))
        progress_update = self._call_event_handler(
            'add_progress_update', message, initial_step=initial_step,
            total_steps=total_steps, return_event=True)
        progress_update_id = (
            self._call_event_handler(
                'get_progress_update_identifier', progress_update))

        perc = 0
        if initial_step > 0 and total_steps > 0:
            perc = int(initial_step * 100 // total_steps)
        self._perc_steps[progress_update_id] = _PercStepData(
                progress_update_id, perc, initial_step, total_steps)

        return self._perc_steps[progress_update_id]

    def set_percentage_step(self, step, new_current_step):
        perc_step = self._perc_steps.get(
                step.progress_update_id, None)
        if perc_step is None:
            return

        if perc_step.last_value > new_current_step:
            LOG.warn("rollback for perc update %s not allowed" % step.progress_update_id)
            return

        perc = 0
        if perc_step.total_steps > 0 and new_current_step > 0:
            perc = int(new_current_step * 100 // perc_step.total_steps)

        if self._call_event_handler and perc > perc_step.last_perc:
            self._call_event_handler(
                'update_progress_update', step.progress_update_id,
                new_current_step)
            perc_id = copy.copy(step.progress_update_id)
            total_steps = perc_step.total_steps
            del self._perc_steps[step.progress_update_id]
            del perc_step
            self._perc_steps[perc_id] = _PercStepData(
                perc_id, perc, 0, total_steps)

    def progress_update(self, message):
        self._call_event_handler(
            'add_progress_update', message, return_event=False)

    def info(self, message):
        self._call_event_handler(
            'add_event', message, level=constants.TASK_EVENT_INFO)

    def warn(self, message):
        self._call_event_handler(
            'add_event', message, level=constants.TASK_EVENT_WARNING)

    def error(self, message):
        self._call_event_handler(
            'add_event', message, level=constants.TASK_EVENT_ERROR)


class BaseEventHandler(object, with_metaclass(abc.ABCMeta)):

    @abc.abstractmethod
    def add_progress_update(
            self, message, initial_step=0, total_steps=0,
            return_event=False):
        pass

    @abc.abstractmethod
    def update_progress_update(
            self, update_identifier, new_current_step,
            new_total_steps=None, new_message=None):
        pass

    @classmethod
    @abc.abstractmethod
    def get_progress_update_identifier(cls, progress_update):
        """ Returns the identifier for a given progress update. """
        pass

    @abc.abstractmethod
    def add_event(self, message, level=constants.TASK_EVENT_INFO):
        pass
