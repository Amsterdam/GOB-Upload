"""
Event applicator

Applies events to the respective entity in the current model

"""
import json

from gobcore.exceptions import GOBException
from gobcore.events import GOB, GobEvent
from gobcore.events.import_message import MessageMetaData

from gobcore.model import GOBModel
from gobcore.model.migrations import GOBMigrations

from gobupload.utils import ActiveGarbageCollection


class EventApplicator:

    MAX_ADD_CHUNK = 10000
    MAX_OTHER_CHUNK = 10000

    def __init__(self, storage, last_events):
        self.storage = storage
        # Use a lookup table to tell if an entity is new to the collection
        self.last_events = last_events
        # Buffer (initial) ADD events and other events
        self.add_events = []
        self.other_events = {}

    def __enter__(self):
        # Initialize buffers
        self.add_events = []
        self.other_events = {}
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Write any buffered entities and flush storage
        self.apply_add_events()
        self.apply_other_events()
        self.storage.force_flush_entities()

    def add_add_event(self, event):
        self.add_events.append(event)
        if len(self.add_events) >= self.MAX_ADD_CHUNK:
            self.apply_add_events()

    def apply_add_events(self):
        if self.add_events:
            with ActiveGarbageCollection("Apply add events"):
                self.storage.add_add_events(self.add_events)
                self.add_events = []

    def add_other_event(self, gob_event, data):
        """
        Add a non-ADD event, or an ADD event on a deleted entity
        If MAX_OTHER_CHUNK events have been buffered then mass-apply the events

        :param gob_event:
        :param data:
        :return:
        """
        self.other_events[data["_entity_source_id"]] = gob_event
        if len(self.other_events) >= self.MAX_OTHER_CHUNK:
            self.apply_other_events()

    def apply_other_events(self):
        """
        Mass-Apply events

        :return:
        """
        if self.other_events:
            with ActiveGarbageCollection("Apply other events"):
                # Get all entities to be updated by their source-id
                source_ids = self.other_events.keys()
                entities = self.storage.get_entities(source_ids, with_deleted=True)
                for entity in entities:
                    self.apply_other_event(entity)
            self.other_events = {}

    def apply_other_event(self, entity):
        """
        Apply an event on an entity

        The event can be an:
        - ADD event (reanimation of a DELETED entity)
        - DELETE or MODIFY event
        - CONFIRM event (these event only set the last modified date, not the last event id)

        :param entity:
        :return:
        """
        gob_event = self.other_events[entity._source_id]

        # Check action validity
        if entity._date_deleted is not None and not isinstance(gob_event, GOB.ADD):
            # a non-ADD event is trying to be applied on a deleted entity
            # Only ADD event can be applied on a deleted entity
            raise GOBException(f"Trying to '{gob_event.name}' a deleted entity")

        # apply the event on the entity
        gob_event.apply_to(entity)

        # and register the last event that has updated this entity
        # except for CONFIRM events. These events are deleted once they have been applied
        if not isinstance(gob_event, GOB.CONFIRM):
            entity._last_event = gob_event.id

    def apply(self, event):
        # Parse the json data of the event
        if isinstance(event.contents, dict):
            data = event.contents
        else:
            data = json.loads(event.contents)

        # Reconstruct the gob event out of the database event
        gob_event = _get_gob_event(event, data)

        # Return the action and number of applied entities
        count = 1

        if isinstance(gob_event, GOB.BULKCONFIRM):
            self.storage.bulk_update_confirms(gob_event, event.eventid)
            count = len(gob_event._data['confirms'])
        elif isinstance(gob_event, GOB.ADD) and self.last_events.get(data["_entity_source_id"]) is None:
            # Initial add (an ADD event can also be applied on a deleted entity, this is handled by the else case)
            self.add_add_event(gob_event)
        else:
            # If ADD events are waiting to be applied to the database, flush those first to make sure they exist
            self.apply_add_events()

            # Add other event (MODIFY, CONFIRM, DELETE, ADD on deleted entity)
            self.add_other_event(gob_event, data)

        return gob_event, count


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
