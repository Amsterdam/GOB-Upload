"""
Event Collector

Stores events in the event table
"""
from gobcore.exceptions import GOBException
from gobupload.storage.handler import GOBStorageHandler


class EventCollector:

    def __init__(self, storage: GOBStorageHandler, last_events: dict[str, int]):
        # Local dictionary that contains the last event number for every source_id
        self.last_events = last_events
        self.events = []
        self.storage = storage

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.events:
            raise GOBException(
                "Have not stored events. Call store_events() before leaving context. "
                f"({exc_val})"
            )

    def store_events(self):
        if self.events:
            self.storage.add_events(self.events)
            self._clear()

    def _clear(self):
        self.events.clear()

    def collect(self, event):
        """
        Checks an event for being valid and stores it in the events table

        :param event:
        :return:
        """
        self.events.append(event)

    def is_valid(self, event) -> bool:
        """
        Tells if an event is valid by matching against the last event of the corresponding entity

        :param event:
        :return:
        """
        event_tid = event["data"]["_tid"]
        event_last_event = event["data"]["_last_event"]
        last_event = self.last_events.get(event_tid)

        return (last_event is None and event["event"] == "ADD") or event_last_event == last_event
