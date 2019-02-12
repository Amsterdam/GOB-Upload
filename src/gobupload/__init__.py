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
