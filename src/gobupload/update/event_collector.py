

class EventCollector:

    def __init__(self, storage):
        self.storage = storage
        self.last_events = self.storage.get_last_events()

    def collect(self, event):
        is_valid = self._validate(event)
        if is_valid:
            self.storage.add_event(event)
        return is_valid

    def _validate(self, event):
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

        return False not in [id['last_event'] == self.last_events.get(id['source_id'], None) for id in ids]
