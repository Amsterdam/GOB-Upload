"""
Event applicator

Applies events to the respective entity in the current model

"""
from __future__ import annotations

import datetime
from collections import defaultdict
from typing import Union

from gobcore.events import GOB, database_to_gobevent, ImportEvent
from gobcore.exceptions import GOBException
from gobcore.model import FIELD
from gobupload.storage.handler import GOBStorageHandler
from gobupload.update.update_statistics import UpdateStatistics


class EventApplicator:

    def __init__(self, storage: GOBStorageHandler, last_events: set[str], stats: UpdateStatistics):
        self.storage = storage
        self.stats = stats

        self.inserts: list[GOB.ADD] = []
        self.updates: dict[str, list[Union[GOB.ADD, GOB.MODIFY, GOB.DELETE]]] = defaultdict(list)
        self.updates_total = 0

        self.last_events = last_events
        self.add_event_tids = set()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.inserts or self.updates:
            raise GOBException("Have unapplied events. Call apply_all() before leaving context")

    def _add_insert(self, gob_event: GOB.ADD):
        """Adds ADD event to buffer."""
        self.inserts.append(gob_event)

    def _add_update(self, gob_event: GOB.ADD | GOB.MODIFY | GOB.DELETE):
        """Add a non-ADD event or an ADD event on a deleted entity."""
        self.updates[gob_event.tid].append(gob_event)
        self.updates_total += 1

    @staticmethod
    def _validate_update_event(gob_event: ImportEvent, date_deleted: datetime.datetime | None):
        if isinstance(gob_event, GOB.ADD):
            # only apply ADD on deleted entity
            if date_deleted is not None:
                return

            # a ADD event is trying to be applied to a non-deleted (current) entity
            raise GOBException(
                f"Trying to 'ADD' an existing (non-deleted) entity. "
                f"(id: {gob_event.id}, last_event: {gob_event.last_event}, tid: {gob_event.tid})"
            )

        else:
            # only apply GOB.MODIFY, GOB.DELETE on non-deleted entity
            if date_deleted is None:
                return

            # a non-ADD event is trying to be applied on a deleted entity
            raise GOBException(
                f"Trying to '{gob_event.name}' a deleted entity "
                f"(id: {gob_event.id}, last_event: {gob_event.last_event}) tid: {gob_event.tid})"
            )

    def _flush_updates(self):
        """Generate database updates from events in buffer and clear buffer."""
        if self.updates:
            for entity in self.storage.get_entities(self.updates.keys(), with_deleted=True):
                self._update_entity(entity)

            self.updates.clear()
            self.updates_total = 0

    def _update_entity(self, entity: "GOBStorageHandler.DbEntity"):
        tid = getattr(entity, FIELD.TID)
        events = self.updates[tid]

        for event in events:
            self._validate_update_event(event, getattr(entity, FIELD.DATE_DELETED))

            event.apply_to(entity)
            self.stats.add_applied(event.action, 1)

        if events:
            setattr(entity, FIELD.LAST_EVENT, events[-1].id)

    def _flush_inserts(self):
        """Generate database inserts from ADD events and clear buffer."""
        if self.inserts:
            self.storage.add_add_events(self.inserts)

            self.stats.add_applied(GOB.ADD.name, len(self.inserts))
            self.inserts.clear()

    def flush(self):
        """Generate database statements from events and clear buffer (not committed yet)."""
        self._flush_inserts()
        self._flush_updates()

    def load(self, event):
        """Load the `event` to buffer."""
        # Reconstruct the gob event out of the database event
        gob_event = database_to_gobevent(event)
        tid = gob_event.tid

        if isinstance(gob_event, GOB.ADD) and tid not in self.last_events and tid not in self.add_event_tids:
            # Initial add (an ADD event can also be applied on a deleted entity, this is handled by the else case)
            self._add_insert(gob_event)

            # Store the tid to make sure a second ADD events get's handled as an ADD on deleted entity
            self.add_event_tids.add(tid)

        else:
            # If ADD events are waiting to be applied to the database, flush those first to make sure they exist
            self._flush_inserts()

            # Add other event (MODIFY, DELETE, ADD on deleted entity)
            self._add_update(gob_event)
