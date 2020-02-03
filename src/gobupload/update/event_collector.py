"""
Event Collector

Stores events in the event table
"""


class EventCollector:

    MAX_CHUNK = 10000

    def __init__(self, storage, last_events):
        self.storage = storage
        # Local dictionary that contains the last event number for every source_id
        self.last_events = last_events
        self.events = []

    def __enter__(self):
        self.events = []
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.store_events()

    def add_event(self, event):
        if event['event'] == 'BULKCONFIRM':
            # Bulk events are stored immediately
            return self.storage.add_events([event])

        self.events.append(event)
        if len(self.events) >= self.MAX_CHUNK:
            # All other events are grouped per 10000
            return self.store_events()

    def store_events(self):
        if len(self.events):
            self.storage.add_events(self.events)
            self.events = []

    def collect(self, event):
        """
        Checks an event for being valid and stores it in the events table

        :param event:
        :return: True is the event was valid and stored
        """
        is_valid = self._validate(event)
        if is_valid:
            self.add_event(event)
        return is_valid

    def _match_last_event(self, id, event_type):
        """
        Tells if an event matches with the last event of the corresponding entity

        The compare step tells the last event of the entity against which is compared
        If this matches with the current last event of the entity the event is valid
        :param id:
        :param event_type:
        :return:
        """
        last_event = self.last_events.get(id['source_id'])
        return id['last_event'] == last_event or (event_type == 'ADD' and last_event is None)

    def _validate(self, event):
        """
        Tells if an event is valid by matching against the last event of the corresponding entity

        :param event:
        :return:
        """
        event_type = event['event']
        if event_type == 'BULKCONFIRM':
            ids = [{
                'source_id': confirm['_source_id'],
                'last_event': confirm['_last_event']
            } for confirm in event['data']['confirms']]
        else:
            ids = [{
                'source_id': event['data']['_entity_source_id'],
                'last_event': event['data']['_last_event']
            }]

        is_valid = all([self._match_last_event(id, event_type) for id in ids])
        if not is_valid:
            print("Invalid event", event, [self.last_events.get(id['source_id']) for id in ids])
        return is_valid
