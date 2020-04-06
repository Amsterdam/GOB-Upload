"""
Event applicator

Applies events to the respective entity in the current model

"""
import json

from gobcore.events import GOB, GobEvent
from gobcore.events.import_message import MessageMetaData

from gobcore.model import GOBModel
from gobcore.model.migrations import GOBMigrations

from gobupload.utils import ActiveGarbageCollection


class EventApplicator:

    MAX_ADD_CHUNK = 10000

    def __init__(self, storage, last_events):
        self.storage = storage
        # Use a lookup table to tell if an entity is new to the collection
        self.last_events = last_events
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
            with ActiveGarbageCollection("Apply add events"):
                self.storage.add_add_events(self.add_events)
                self.add_events = []

    def apply(self, event):
        # Parse the json data of the event
        if isinstance(event.contents, dict):
            data = event.contents
        else:
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
        elif isinstance(gob_event, GOB.ADD) and self.last_events.get(data["_entity_source_id"]) is None:
            # Initial add
            self.add_add_event(gob_event)
        else:
            # If ADD events are waiting to be applied to the database, flush those first to make sure they exist
            self.apply_add_events()

            # Get the entity to which the event should be applied, create if ADD event
            entity = self.storage.get_entity_for_update(event, data)

            # apply the event on the entity
            gob_event.apply_to(entity)

            # and register the last event that has updated this entity
            # except for CONFIRM events. These events are deleted once they have been applied
            if not isinstance(gob_event, GOB.CONFIRM):
                entity._last_event = event.eventid

        return action, count


def _get_gob_event(event, data):
    """Reconstruct the original event out of the stored event

    :param event: the database event
    :param data: the data that is associated with the event
    :return: a ADD, MODIFY, CONFIRM or DELETE event
    """

    # Get the model version to check if the event should be migrated to the correct version
    model_version = GOBModel().get_collection(event.catalogue, event.entity)['version']

    if model_version != event.version:
        # Event should be migrated to the correct GOBModel version
        data = GOBMigrations().migrate_event_data(event, data, event.catalogue, event.entity, model_version)

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
