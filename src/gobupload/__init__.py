EVENT_LOG_MAPPING = {
    'ADD': 'num_added',
    'CONFIRM': 'num_confirmed',
    'DELETE': 'num_deleted',
    'MODIFY': 'num_modified',
}


def get_report(events):
    """
    Return a simple report telling how many of each events has been processed

    :param events:
    :return: dict with number of events per gob event
    """
    counted_events = {
        'num_records': len(events)
    }
    for event in events:
        event_log = EVENT_LOG_MAPPING[event['event']]
        try:
            counted_events[event_log] += 1
        except KeyError:
            counted_events[event_log] = 1
    return counted_events
