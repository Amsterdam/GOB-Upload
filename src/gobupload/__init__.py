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


class Logger():

    _logger = {}

    def __init__(self, name, default_args):
        self._name = name
        self._default_args = default_args
        if Logger._logger.get(name) is None:
            Logger._logger[name] = get_logger(name)

    def info(self, msg, kwargs={}):
        Logger._logger[self._name].info(msg, extra={**(self._default_args), **kwargs})

    def warning(self, msg, kwargs={}):
        Logger._logger[self._name].warning(msg, extra={**(self._default_args), **kwargs})

    def error(self, msg, kwargs={}):
        Logger._logger[self._name].error(msg, extra={**(self._default_args), **kwargs})


def init_logger(msg, name):
    """Provide for a logger for this message

    :param msg: the processed message
    :param name: the name of the process
    :return: Logger
    """
    default_args = {
        'process_id': msg['header']['process_id'],
        'source': msg['header']['source'],
        'application': msg['header']['application'],
        'catalogue': msg['header']['catalogue'],
        'entity': msg['header']['entity']
    }
    return Logger(name, default_args)
