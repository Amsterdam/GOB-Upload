from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobupload.storage.handler import GOBStorageHandler
from gobupload.update.event_collector import EventCollector


class TestEventCollector(TestCase):

    def setUp(self):
        self.storage = MagicMock(spec=GOBStorageHandler)

    def tearDown(self):
        pass

    def test_constructor(self):
        collector = EventCollector(self.storage)
        self.assertEqual(collector.events, [])

    def test_add_regular_event(self):
        collector = EventCollector(self.storage)
        collector.add_event({'event': 'any event'})
        self.assertEqual(collector.events, [{'event': 'any event'}])

    def test_add_bulk_event(self):
        collector = EventCollector(self.storage)
        collector.add_event({'event': 'BULKCONFIRM'})
        self.assertEqual(collector.events, [])
        print(self.storage.mock_calls)
        self.storage.add_events.assert_called_with([{'event': 'BULKCONFIRM'}])

    def test_validate(self):
        self.storage.get_last_events.return_value = {
            1: 100
        }
        collector = EventCollector(self.storage)

        event = {
            'event': 'event',
            'data': {
                '_entity_source_id': 1,
                '_last_event': 100
            }
        }
        result = collector._validate(event)
        self.assertEqual(result, True)

        event['data']['_last_event'] = 50
        result = collector._validate(event)
        self.assertEqual(result, False)

        event['data']['_entity_source_id'] = 2
        result = collector._validate(event)
        self.assertEqual(result, False)

    def test_validate_bulk(self):
        self.storage.get_last_events.return_value = {
            1: 100
        }
        collector = EventCollector(self.storage)

        event = {
            'event': 'BULKCONFIRM',
            'data': {
                'confirms': [{
                    '_source_id': 1,
                    '_last_event': 100
                }]
            }
        }
        result = collector._validate(event)
        self.assertEqual(result, True)

        event['data']['confirms'][0]['_last_event'] = 50
        result = collector._validate(event)
        self.assertEqual(result, False)

        event['data']['confirms'][0]['_entity_source_id'] = 2
        result = collector._validate(event)
        self.assertEqual(result, False)

    def test_exit(self):
        EventCollector.MAX_CHUNK = 2
        collector = EventCollector(self.storage)
        collector.add_event({'event': 'any event'})
        self.assertEqual(collector.events, [{'event': 'any event'}])
        collector.add_event({'event': 'any event'})
        self.assertEqual(collector.events, [])
        self.storage.add_events.assert_called_with([{'event': 'any event'}, {'event': 'any event'}])


