from gobcore.events.import_events import CONFIRM

from gobupload.utils import is_corrupted, get_event_ids
from gobupload.storage.handler import GOBStorageHandler

from gobupload.apply.event_applicator import _get_gob_event


class ApplyException(Exception):
    pass


def apply_unhandled_events(storage: GOBStorageHandler) -> int:
    # Get max entity last event and max event
    entity_max_eventid, last_eventid = get_event_ids(storage)
    if is_corrupted(entity_max_eventid, last_eventid):
        raise ApplyException(f"Model is inconsistent: entity@{entity_max_eventid} events@{last_eventid}")
    elif entity_max_eventid != last_eventid:
        # Apply any unhandled events
        apply_events(storage, entity_max_eventid)
        # Re-get max entity last event and max event
        entity_max_eventid, last_eventid = get_event_ids(storage)
        # Storage should now be up-to-date
        assert entity_max_eventid == last_eventid
    return entity_max_eventid


def apply_events(storage: GOBStorageHandler, entity_max_eventid: int) -> None:
    with storage.get_session():
        unhandled_events = storage.get_events_starting_after(entity_max_eventid)
        for event in unhandled_events:
            data = event.contents
            gob_event = _get_gob_event(event, data)
            entity = storage.get_entity_for_update(event, data)
            if entity._last_event and entity._last_event > entity_max_eventid:
                raise ApplyException(f"Model is inconsistent: entity@{entity._last_event} > {entity_max_eventid}")
            gob_event.apply_to(entity)
            entity._last_event = event.eventid


def apply_confirms(storage: GOBStorageHandler, events, timestamp: str) -> None:
    if [event['event'] for event in events if event['event'] != CONFIRM.name]:
        raise ApplyException("Only CONFIRM events are not stored")

    confirm_data = [event['data'] for event in events]
    storage.apply_confirms(confirm_data, timestamp)
