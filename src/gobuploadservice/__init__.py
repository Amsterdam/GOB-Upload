from gobcore.events import GOB_EVENTS


def print_report(events):
    """
    Print a simple report telling how many of each events has been processed

    :param events:
    :return:
    """
    print(f"Aantal mutaties: {len(events)}")
    for gob_event in GOB_EVENTS:
        counted_events = [event for event in events if event['event'] == gob_event.name]
        if len(counted_events) > 0:
            print(f"- {gob_event.name}: {len(counted_events)}")
