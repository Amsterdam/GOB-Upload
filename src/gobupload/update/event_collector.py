"""
Event Collector

Stores events in the event table
"""


class EventCollector:

    def __init__(self, storage):
        self.storage = storage
        # Local dictionary that contains the last event number for every source_id
        self.last_events = self.storage.get_last_events()

    def collect(self, event):
        """
        Checks an event for being valid and stores it in the events table

        :param event:
        :return: True is the event was valid and stored
        """
        is_valid = self._validate(event)
        if is_valid:
            self.storage.add_event(event)
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

        return False not in [self._match_last_event(id, event_type) for id in ids]
