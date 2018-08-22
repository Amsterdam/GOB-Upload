"""GOB Events

The event definitions that are defined in the gob_events module are

GOB events are the add, modification, deletion and confirmation of data
The possible events are defined in this module.
The definition and characteristics of each event is in the gob_events module

"""
from gobcore.events import gob_events

# The possible events are imported from the gob_events module
GOB = gob_events

# The actual events that are used within GOB
GOB_EVENTS = [
    GOB.ADD,
    GOB.DELETED,
    GOB.MODIFIED,
    GOB.CONFIRMED
]

# Convert GOB_EVENTS to a dictionary indexed by the name of the event
_gob_events_dict = {event.name: event for event in GOB_EVENTS}


def get_event(name):
    """
    Get the event definition for a given event name

    Example:
        get_gob_event("ADD") => GOBAction:ADD

    :param name:
    :return: the event definition (class) for the given event name
    """
    return _gob_events_dict[name]
