"""
Event Collector

Stores events in the event table
"""
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobupload.storage.handler import GOBStorageHandler


class EventCollector:

    def __init__(self, storage: GOBStorageHandler, last_events: dict[str, str]):
        # Local dictionary that contains the last event number for every source_id
        self.last_events = last_events
        self.events = []
        self.storage = storage

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.events:
            raise GOBException("Have not stored events. Call store_events() before leaving context")

    def store_events(self):
        if self.events:
            self.storage.add_events(self.events)
            self.events.clear()

    def collect(self, event):
        """
        Checks an event for being valid and stores it in the events table

        :param event:
        :return:
        """
        self.events.append(event)

    def _match_last_event(self, id_, event_type) -> bool:
        """
        Tells if an event matches with the last event of the corresponding entity

        The compare step tells the last event of the entity against which is compared
        If this matches with the current last event of the entity the event is valid
        :param id_:
        :param event_type:
        :return:
        """
        last_event = self.last_events.get(id_['tid'])
        return id_['last_event'] == last_event or (event_type == 'ADD' and last_event is None)

    def is_valid(self, event) -> bool:
        """
        Tells if an event is valid by matching against the last event of the corresponding entity

        :param event:
        :return:
        """
        obj = {"tid": event["data"]["_tid"], "last_event": event["data"]["_last_event"]}
        is_valid = self._match_last_event(obj, event["event"])

        if not is_valid:
            logger.error("Invalid event", event, self.last_events.get(obj["tid"]))

        return is_valid
