"""
Event applicator

Applies events to the respective entity in the current model

"""
from collections import defaultdict
from typing import Union

from gobcore.exceptions import GOBException
from gobcore.events import GOB, database_to_gobevent, ImportEvent
from gobupload.storage.handler import GOBStorageHandler
from gobupload.update.update_statistics import UpdateStatistics


class EventApplicator:

    def __init__(self, storage: GOBStorageHandler, last_events: set[str], stats: UpdateStatistics):
        self.storage = storage
        self.stats = stats

        self.add_events: list[GOB.ADD] = []
        self.other_events: dict[str, list[Union[GOB.ADD, GOB.MODIFY, GOB.DELETE]]] = defaultdict(list)
        self.other_events_sum = 0

        self.last_events = last_events
        self.add_event_tids = set()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.add_events or self.other_events:
            raise GOBException("Have unapplied events. Call apply_all() before leaving context")

    def add_add_event(self, event):
        """Adds ADD event to buffer."""
        self.add_events.append(event)

    def apply_add_events(self):
        """Applies ADD events in buffer and clears afterwards."""
        if self.add_events:
            self.storage.add_add_events(self.add_events)

            self.stats.add_applied(GOB.ADD.name, len(self.add_events))
            self.add_events.clear()

    def add_other_event(self, gob_event, tid):
        """Add a non-ADD event or an ADD event on a deleted entity."""
        self.other_events[tid].append(gob_event)
        self.other_events_sum += 1

    def apply_other_events(self):
        """
        Mass-Apply events in buffer and clear them afterwards.
        Initialise a session when applying and close to flush
        """
        if self.other_events:
            entities = self.storage.get_entities(self.other_events.keys(), with_deleted=True)

            for entity in entities:
                self.apply_other_event(entity)

            self.other_events.clear()
            self.other_events_sum = 0

    def _validate_other_event(self, gob_event: ImportEvent, entity) -> None:
        if isinstance(gob_event, GOB.ADD):
            # only apply ADD on deleted entity
            if entity._date_deleted is not None:
                return

            # a ADD event is trying to be applied to a non-deleted (current) entity
            raise GOBException(
                f"Trying to 'ADD' an existing (non-deleted) entity. "
                f"(id: {gob_event.id}, last_event: {gob_event.last_event}, tid: {gob_event.tid})"
            )

        else:
            # only apply GOB.MODIFY, GOB.DELETE on non-deleted entity
            if entity._date_deleted is None:
                return

            # a non-ADD event is trying to be applied on a deleted entity
            raise GOBException(
                f"Trying to '{gob_event.name}' a deleted entity "
                f"(id: {gob_event.id}, last_event: {gob_event.last_event}) tid: {gob_event.tid})"
            )

    def apply_other_event(self, entity):
        """
        Apply an event on an entity

        The event can be an:
        - ADD event (reanimation of a DELETED entity)
        - DELETE or MODIFY event

        :param entity:
        """
        for gob_event in self.other_events.pop(entity._tid):
            # validate event and entity, raises if not valid
            self._validate_other_event(gob_event, entity)

            # apply the event on the entity
            gob_event.apply_to(entity)

            # register the last event that has updated this entity
            entity._last_event = gob_event.id

            # update stats
            self.stats.add_applied(gob_event.action, 1)

    def apply_all(self):
        self.apply_add_events()
        self.apply_other_events()

    def apply(self, event):
        # Reconstruct the gob event out of the database event
        gob_event = database_to_gobevent(event)
        tid = gob_event.tid

        if isinstance(gob_event, GOB.ADD) and tid not in self.last_events and tid not in self.add_event_tids:
            # Initial add (an ADD event can also be applied on a deleted entity, this is handled by the else case)
            self.add_add_event(gob_event)

            # Store the tid to make sure a second ADD events get's handled as an ADD on deleted entity
            self.add_event_tids.add(tid)

        else:
            # If ADD events are waiting to be applied to the database, flush those first to make sure they exist
            self.apply_add_events()

            # Add other event (MODIFY, DELETE, ADD on deleted entity)
            self.add_other_event(gob_event, tid)
