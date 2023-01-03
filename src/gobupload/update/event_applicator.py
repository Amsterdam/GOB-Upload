"""
Event applicator

Applies events to the respective entity in the current model

"""
from collections import defaultdict

from gobcore.exceptions import GOBException
from gobcore.events import GOB, database_to_gobevent
from gobupload.storage.handler import GOBStorageHandler


class EventApplicator:
    MAX_ADD_CHUNK = 10_000
    MAX_OTHER_CHUNK = 2_500

    def __init__(self, storage: GOBStorageHandler, last_events: set[str], add_event_tids: set[str]):
        self.storage = storage

        self.add_events = []
        self.other_events = defaultdict(list)
        self.other_events_sum = 0

        self.last_events = last_events
        self.add_event_tids = add_event_tids

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Write any buffered entities and flush storage
        if self.add_events or self.other_events:
            raise GOBException("Have unapplied events. Call apply_all() before leaving context")
        self.storage.force_flush_entities()

    def add_add_event(self, event):
        """Adds add event to buffer, applies pending add events when buffer is full.
        Returns the list of applied events.

        :param event:
        :return:
        """
        self.add_events.append(event)

        if len(self.add_events) >= self.MAX_ADD_CHUNK:
            self.apply_add_events()

    def apply_add_events(self):
        """Applies add events in buffer.
        Returns the list of applied events

        :return:
        """
        if self.add_events:
            self.storage.add_add_events(self.add_events)
            self.add_events.clear()

    def add_other_event(self, gob_event, tid):
        """
        Add a non-ADD event, or an ADD event on a deleted entity
        If MAX_OTHER_CHUNK events have been buffered then mass-apply the events

        Returns the list of applied events

        :param gob_event:
        :param data:
        :return:
        """
        self.other_events[tid].append(gob_event)
        self.other_events_sum += 1

        if self.other_events_sum >= self.MAX_OTHER_CHUNK:
            self.apply_other_events()

    def apply_other_events(self):
        """
        Mass-Apply events

        Returns the list of applied events

        :return:
        """
        if self.other_events:
            entities = self.storage.get_entities(self.other_events.keys(), with_deleted=True)

            for entity in entities:
                self.apply_other_event(entity)

            self.other_events.clear()
            self.other_events_sum = 0

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
        for gob_event in self.other_events.pop(entity._tid):
            # Check action validity
            if entity._date_deleted is not None and not isinstance(gob_event, GOB.ADD):
                # a non-ADD event is trying to be applied on a deleted entity
                # Only ADD event can be applied on a deleted entity
                raise GOBException(f"Trying to '{gob_event.name}' a deleted entity (id: {gob_event.id}, "
                                   f"last_event: {gob_event.last_event})")

            # apply the event on the entity
            gob_event.apply_to(entity)

            # and register the last event that has updated this entity
            # except for CONFIRM events. These events are deleted once they have been applied
            if not isinstance(gob_event, GOB.CONFIRM):
                entity._last_event = gob_event.id

    def apply_all(self):
        self.apply_add_events()
        self.apply_other_events()

    def apply(self, event):
        # Reconstruct the gob event out of the database event
        gob_event = database_to_gobevent(event)

        # Return the action and number of applied entities
        count = 1
        tid = gob_event.tid

        if isinstance(gob_event, GOB.BULKCONFIRM):
            self.storage.bulk_update_confirms(gob_event, event.eventid)
            count = len(gob_event._data['confirms'])

        elif isinstance(gob_event, GOB.ADD) and tid not in self.last_events and tid not in self.add_event_tids:
            # Initial add (an ADD event can also be applied on a deleted entity, this is handled by the else case)
            self.add_add_event(gob_event)

            # Store the tid to make sure a second ADD events get's handled as an ADD on deleted entity
            self.add_event_tids.add(tid)

        else:
            # If ADD events are waiting to be applied to the database, flush those first to make sure they exist
            self.apply_add_events()

            # Add other event (MODIFY, CONFIRM, DELETE, ADD on deleted entity)
            self.add_other_event(gob_event, tid)

        return gob_event, count
