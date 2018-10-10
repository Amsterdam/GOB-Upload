def get_report(events):
    """
    Return a simple report telling how many of each events has been processed

    :param events:
    :return: dict with number of events per gob event
    """
    counted_events = {
        'RECORDS': len(events)
    }
    for event in events:
        try:
            counted_events[event['event']] += 1
        except KeyError:
            counted_events[event['event']] = 1
    return counted_events
