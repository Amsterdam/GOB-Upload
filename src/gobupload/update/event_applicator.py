"""
Event applicator

Applies events to the respective entity in the current model

"""
import json

from gobcore.events import GOB, GobEvent
from gobcore.events.import_message import MessageMetaData


class EventApplicator:

    MAX_ADD_CHUNK = 10000

    def __init__(self, storage):
        self.storage = storage
        # Use a lookup table to tell if an entity is new to the collection
        # New entities are added in bulk
        self.source_ids = self.storage.get_current_ids(exclude_deleted=False)
        self.add_events = []

    def __enter__(self):
        self.add_events = []
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.apply_add_events()

    def add_add_event(self, event):
        self.add_events.append(event)
        if len(self.add_events) >= self.MAX_ADD_CHUNK:
            self.apply_add_events()

    def apply_add_events(self):
        if self.add_events:
            self.storage.add_add_events(self.add_events)
            self.add_events = []

    def apply(self, event):
        # Parse the json data of the event
        data = json.loads(event.contents)

        # Reconstruct the gob event out of the database event
        gob_event = _get_gob_event(event, data)

        # Return the action and number of applied entities
        action = event.action
        count = 1

        if isinstance(gob_event, GOB.BULKCONFIRM):
            self.storage.bulk_update_confirms(gob_event, event.eventid)
            action = "CONFIRM"
            count = len(gob_event._data['confirms'])
        elif isinstance(gob_event, GOB.ADD) and data["_entity_source_id"] not in self.source_ids:
            self.add_add_event(gob_event)
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
