"""
Event collector

Collects events and groups these events in bulk events when possible
"""
from gobcore.events import GOB
from gobcore.model.metadata import FIELD


class EventCollector:

    MAX_BULK = 10_000           # Max number of events of same type in one bulk event
    BULK_TYPES = ["CONFIRM"]    # Only CONFIRM events are grouped in bulk events

    def __init__(self, contents_writer, confirms_writer, version):
        """
        Initializes the collector with empty collections

        """
        self._bulk_events = []
        self._last_type = None
        self.contents_writer = contents_writer
        self.confirms_writer = confirms_writer
        self.version = version

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._end_of_type()

    def _add_event(self, event):
        """
        Add an event to the list of events

        :param event:
        :return:
        """
        if event['event'] in ['CONFIRM', 'BULKCONFIRM']:
            self.confirms_writer.write(event)
        else:
            self.contents_writer.write(event)

    def _add_bulk_event(self, event):
        """
        Add an event to a collection of events that will be grouped in a bulk event

        :param event:
        :return:
        """
        self._bulk_events.append(event)
        if len(self._bulk_events) >= self.MAX_BULK:
            self._end_of_bulk()

    def _end_of_bulk(self):
        """
        Compact events of same type in one BULK event

        :return:
        """
        if len(self._bulk_events) > 1:
            event = GOB.BULKCONFIRM.create_event([
                {
                    '_tid': event["data"]["_tid"],
                    '_last_event': event["data"]["_last_event"]
                } for event in self._bulk_events
            ], self.version)
            self._add_event(event)
        else:
            self._add_event(self._bulk_events[0])
        self._bulk_events = []

    def _end_of_type(self):
        """
        Called on any change of event type. Any open bulk event collection will be closed

        :return:
        """
        if len(self._bulk_events):
            self._end_of_bulk()

    def collect_initial_add(self, entity):
        tid = entity[FIELD.TID]
        event = GOB.ADD.create_event(tid, entity, self.version)
        self.collect(event)

    def collect(self, event):
        """
        Add an event. Handle any grouping of events

        :param event:
        :return:
        """
        event_type = event["event"]

        if self._last_type is not None and self._last_type != event_type:
            self._end_of_type()

        if event_type in self.BULK_TYPES:
            self._add_bulk_event(event)
        else:
            self._add_event(event)

        self._last_type = event_type
