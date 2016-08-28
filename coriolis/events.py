# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import abc
import collections


_PercStepData = collections.namedtuple(
    "_PercStepData", "last_value max_value perc_threshold message_format")


class EventManager(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, event_handler):
        self._event_handler = event_handler
        self._current_step = 0
        self._total_steps = None
        self._percentage_steps = {}

    def set_total_progress_steps(self, total_steps):
        self._total_steps = total_steps

    def add_percentage_step(self, max_value, perc_threshold=1,
                            message_format="{:.0f}%"):
        self._current_step += 1
        self._percentage_steps[self._current_step] = _PercStepData(
            0, max_value, perc_threshold, message_format)
        return self._current_step

    def set_percentage_step(self, step, value):
        step_data = self._percentage_steps[step]

        old_perc = (step_data.last_value * 100 / step_data.max_value //
                    step_data.perc_threshold * step_data.perc_threshold)
        perc = (value * 100 / step_data.max_value //
                step_data.perc_threshold * step_data.perc_threshold)

        if perc > old_perc and self._event_handler:
            self._event_handler.progress_update(
                step, self._total_steps, step_data.message_format.format(perc))
            self._percentage_steps[step] = _PercStepData(
                value, step_data.max_value, step_data.perc_threshold,
                step_data.message_format)

    def progress_update(self, message):
        self._current_step += 1
        if self._event_handler:
            self._event_handler.progress_update(
                self._current_step, self._total_steps, message)

    def info(self, message):
        if self._event_handler:
            self._event_handler.info(message)

    def warn(self, message):
        if self._event_handler:
            self._event_handler.warn(message)

    def error(self, message):
        if self._event_handler:
            self._event_handler.error(message)


class BaseEventHandler(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def progress_update(self, current_step, total_steps, message):
        pass

    @abc.abstractmethod
    def info(self, message):
        pass

    @abc.abstractmethod
    def warn(self, message):
        pass

    @abc.abstractmethod
    def error(self, message):
        pass
