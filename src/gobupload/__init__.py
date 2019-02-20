def get_report(contents, events):
    """
    Return a simple report telling how many of each events has been processed

    :param contents: total number of messages
    :param events: event messages
    :return: dict with number of events per gob event
    """
    EVENT_LOG_MAPPING = {
        'ADD': 'num_added',
        'CONFIRM': 'num_confirmed',
        'BULKCONFIRM': 'num_confirmed',
        'DELETE': 'num_deleted',
        'MODIFY': 'num_modified',
    }

    counted_events = {}
    total_count = 0

    for event in events:
        event_log = EVENT_LOG_MAPPING[event['event']]
        if event['event'] == 'BULKCONFIRM':
            confirms = len(event['data']['confirms'])
            counted_events[event_log] = counted_events.get(event_log, 0) + confirms
            total_count += confirms
        else:
            counted_events[event_log] = counted_events.get(event_log, 0) + 1
            total_count += 1

    counted_events['num_records'] = total_count
    skipped = len(contents) - total_count
    if skipped > 0:
        counted_events['num_skipped_historical'] = skipped

    return counted_events
