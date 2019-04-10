import json

from gobcore.events import GOB, GobEvent
from gobcore.events.import_message import MessageMetaData


class EventApplicator:

    def __init__(self, storage):
        self.storage = storage

    def apply(self, event):
        # Parse the json data of the event
        data = json.loads(event.contents)

        # Reconstruct the gob event out of the database event
        gob_event = _get_gob_event(event, data)

        action = event.action
        count = 1

        if isinstance(gob_event, GOB.BULKCONFIRM):
            self.storage.bulk_update_confirms(gob_event, event.eventid)
            action = "CONFIRM"
            count = len(gob_event._data['confirms'])
        else:
            # Get the entity to which the event should be applied, create if ADD event
            entity = self.storage.get_entity_for_update(event, data)

            # apply the event on the entity
            gob_event.apply_to(entity)

            # and register the last event that has updated this entity
            entity._last_event = event.eventid

        return action, count


def _get_gob_event(event, data):
    """Reconstruct the original event out of the stored event

    :param event: the database event
    :param data: the data that is associated with the event
    :return: a ADD, MODIFY, CONFIRM or DELETE event
    """

    event_msg = {
        "event": event.action,
        "data": data
    }

    msg_header = {
        "process_id": None,
        "source": event.source,
        "application": event.application,
        "id_column": data.get("id_column"),
        "catalogue": event.catalogue,
        "entity": event.entity,
        "version": event.version,
        "timestamp": event.timestamp
    }

    # Construct the event out of the reconstructed event data
    gob_event = GobEvent(event_msg, MessageMetaData(msg_header))

    # Store the id of the event in the gob_event
    gob_event.id = event.eventid
    return gob_event
