"""
Event applicator

Applies events to the respective entity in the current model

"""
from collections import defaultdict

from gobcore.exceptions import GOBException
from gobcore.events import GOB, database_to_gobevent

from gobupload.utils import ActiveGarbageCollection


class EventApplicator:

    MAX_ADD_CHUNK = 10000
    MAX_OTHER_CHUNK = 10000

    def __init__(self, storage, last_events):
        self.storage = storage
        # Use a lookup table to tell if an entity is new to the collection
        self.last_events = last_events

        self._initialize_buffers()

    def __enter__(self):
        self._initialize_buffers()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Write any buffered entities and flush storage
        if self.add_events or self.other_events:
            raise GOBException(f"Have unapplied events. Call apply_all() before leaving context")
        self.storage.force_flush_entities()

    def _initialize_buffers(self):
        self.add_events = []
        self.add_event_source_ids = set()
        self.other_events = defaultdict(list)

    def add_add_event(self, event):
        """Adds add event to buffer, applies pending add events when buffer is full.
        Returns the list of applied events.

        :param event:
        :return:
        """
        self.add_events.append(event)
        if len(self.add_events) >= self.MAX_ADD_CHUNK:
            return self.apply_add_events()
        return []

    def apply_add_events(self):
        """Applies add events in buffer.
        Returns the list of applied events

        :return:
        """
        if self.add_events:
            with ActiveGarbageCollection("Apply add events"):
                self.storage.add_add_events(self.add_events)
                applied_events = self.add_events
                self.add_events = []
                return applied_events
        return []

    def add_other_event(self, gob_event, data):
        """
        Add a non-ADD event, or an ADD event on a deleted entity
        If MAX_OTHER_CHUNK events have been buffered then mass-apply the events

        Returns the list of applied events

        :param gob_event:
        :param data:
        :return:
        """
        self.other_events[data["_entity_source_id"]].append(gob_event)
        if sum([len(x) for x in self.other_events.values()]) >= self.MAX_OTHER_CHUNK:
            return self.apply_other_events()
        return []

    def apply_other_events(self):
        """
        Mass-Apply events

        Returns the list of applied events

        :return:
        """
        if self.other_events:
            with ActiveGarbageCollection("Apply other events"):
                # Get all entities to be updated by their source-id
                source_ids = self.other_events.keys()
                entities = self.storage.get_entities(source_ids, with_deleted=True)
                for entity in entities:
                    self.apply_other_event(entity)
            applied_events = [event for events in self.other_events.values() for event in events]
            self.other_events = defaultdict(list)
            return applied_events
        return []

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
        gob_events = self.other_events[entity._source_id]

        for gob_event in gob_events:
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

    def apply_all(self):
        applied = self.apply_add_events()
        applied += self.apply_other_events()
        return applied

    def apply(self, event):
        # Reconstruct the gob event out of the database event
        gob_event = database_to_gobevent(event)
        data = gob_event.data

        # Return the action and number of applied entities
        count = 1
        applied_events = []

        entity_source_id = data.get('_entity_source_id')

        if isinstance(gob_event, GOB.BULKCONFIRM):
            self.storage.bulk_update_confirms(gob_event, event.eventid)
            count = len(gob_event._data['confirms'])
        elif isinstance(gob_event, GOB.ADD) and self.last_events.get(entity_source_id) is None and \
                entity_source_id not in self.add_event_source_ids:
            # Initial add (an ADD event can also be applied on a deleted entity, this is handled by the else case)
            applied_events += self.add_add_event(gob_event)

            # Store the entity_source_id to make sure a second ADD events get's handled as an ADD on deleted entity
            self.add_event_source_ids.add(data.get('_entity_source_id'))
        else:
            # If ADD events are waiting to be applied to the database, flush those first to make sure they exist
            applied_events += self.apply_add_events()

            # Add other event (MODIFY, CONFIRM, DELETE, ADD on deleted entity)
            applied_events += self.add_other_event(gob_event, data)

        return gob_event, count, applied_events
