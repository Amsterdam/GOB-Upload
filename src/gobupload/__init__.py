from gobcore.log import get_logger


def get_report(contents, events, recompares):
    """
    Return a simple report telling how many of each events has been processed

    :param contents: total number of messages
    :param events: event messages
    :param recompares: recompare messages
    :return: dict with number of events per gob event
    """
    EVENT_LOG_MAPPING = {
        'ADD': 'num_added',
        'CONFIRM': 'num_confirmed',
        'DELETE': 'num_deleted',
        'MODIFY': 'num_modified',
    }

    counted_events = {
        'num_records': len(events),
    }
    skipped = len(contents) - len(events) - len(recompares)
    if skipped > 0:
        counted_events['num_skipped_historical'] = skipped
    if len(recompares) > 0:
        counted_events['num_recompare'] = len(recompares)
    for event in events:
        event_log = EVENT_LOG_MAPPING[event['event']]
        counted_events[event_log] = counted_events.get(event_log, 0) + 1
    return counted_events


class Logger:
    """Singleton Logger wrapper class

    Holds information to give context to subsequent logging.
    """
    __inst_loggers = {}

    def __init__(self, name, default_args):
        """Initialize logger

        :param msg: the processed message
        :param name: name of the logger
        :return: Logger
        """
        if name in Logger.__inst_loggers:
            raise Exception  # Todo change Exception with more info

        self._name = name
        self.default_args = default_args
        self._logger = get_logger(name)

        Logger.__inst_loggers[name] = self

    def info(self, msg, kwargs={}):
        self._logger.info(msg, extra={**self.default_args, **kwargs})

    def warning(self, msg, kwargs={}):
        self._logger.warning(msg, extra={**self.default_args, **kwargs})

    def error(self, msg, kwargs={}):
        self._logger.error(msg, extra={**self.default_args, **kwargs})

    @staticmethod
    def init_logger(msg, name="INFO"):
        """Initialize logger with extra data from msg.
        If logger with name already is initialized then default_args are updated on that logger
        with data from msg.

        :param msg: name of the logger
        :param name: The name of the logger instance. This name will be part of every log record
        :return: Logger
        """
        default_args = {
            'process_id': msg['header']['process_id'],
            'source': msg['header']['source'],
            'application': msg['header']['application'],
            'catalogue': msg['header']['catalogue'],
            'entity': msg['header']['entity']
        }
        if name in Logger.__inst_loggers:
            Logger.__inst_loggers[name].default_args = default_args
            return Logger.__inst_loggers[name]
        return Logger(name, default_args)

    @staticmethod
    def get_logger(name="INFO"):
        """Gets for a logger with name.
        If no logger with name is present an new instance is setup.

        :param name: The name of the logger instance. This name will be part of every log record
        :return: Logger
        """
        if name not in Logger.__inst_loggers:
            Logger(name, {})
        return Logger.__inst_loggers[name]
