from gobcore.events import GOB


class EventCollector:

    MAX_BULK = 10000
    BULK_TYPES = ["CONFIRM"]

    def __init__(self):
        self.events = []
        self._bulk_events = []
        self._last_type = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._end_of_type()

    def _add_event(self, event):
        self.events.append(event)

    def _add_bulk_event(self, event):
        self._bulk_events.append(event)
        if len(self._bulk_events) >= self.MAX_BULK:
            self._end_of_bulk()

    def _end_of_bulk(self):
        # Compact events of same type in one BULK event
        # Currently only for CONFIRM events
        event = GOB.BULKCONFIRM.create_event([
            {
                '_source_id': event["data"]["_source_id"],
                '_last_event': event["data"]["_last_event"]
            } for event in self._bulk_events
        ])
        self._add_event(event)
        self._bulk_events = []

    def _end_of_type(self):
        if len(self._bulk_events):
            self._end_of_bulk()

    def add(self, event):
        event_type = event["event"]

        if self._last_type is not None and self._last_type != event_type:
            self._end_of_type()

        if event_type in self.BULK_TYPES:
            self._add_bulk_event(event)
        else:
            self._add_event(event)

        self._last_type = event_type
