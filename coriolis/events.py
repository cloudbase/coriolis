import abc


class EventManager(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, event_handler):
        self._event_handler = event_handler
        self._current_step = 0
        self._total_steps = None

    def set_total_progress_steps(self, total_steps):
        self._total_steps = total_steps

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
