from gobcore.events import gob_events

GOB = gob_events

GOB_EVENTS = [GOB.ADD, GOB.DELETED, GOB.MODIFIED, GOB.CONFIRMED]
_gob_events_dict = {event.name: event for event in GOB_EVENTS}


def get_event(name):
    return _gob_events_dict[name]