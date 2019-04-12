import json

from unittest import TestCase
from unittest.mock import MagicMock

from tests.fixtures import dict_to_object

from gobupload.storage.handler import GOBStorageHandler
from gobupload.update.event_applicator import EventApplicator


class TestEventCollector(TestCase):

    def setUp(self):
        self.storage = MagicMock(spec=GOBStorageHandler)
        self.mock_event = {
            'version': '0.1',
            'catalogue': 'test_catalogue',
            'application': 'TEST',
            'entity': 'test_entity',
            'timestamp': None,
            'source': 'test',
            'action': 'ADD',
            'source_id': 'source_id',
            'contents': None
        }

    def tearDown(self):
        pass

    def set_contents(self, contents):
        self.mock_event["contents"] = json.dumps(contents)

    def test_constructor(self):
        applicator = EventApplicator(self.storage)
        self.assertEqual(applicator.add_events, [])

    def test_apply(self):
        applicator = EventApplicator(self.storage)
        self.mock_event["action"] = 'CONFIRM'
        self.set_contents({
            '_entity_source_id': 'entity_source_id',
            '_hash': '123'
        })
        event = dict_to_object(self.mock_event)
        applicator.apply(event)
        self.assertEqual(len(applicator.add_events), 0)

    def test_apply_new_add(self):
        applicator = EventApplicator(self.storage)
        self.set_contents({
            '_entity_source_id': 'entity_source_id',
            '_hash': '123'
        })
        event = dict_to_object(self.mock_event)
        applicator.apply(event)
        self.assertEqual(len(applicator.add_events), 1)

    def test_apply_bulk(self):
        applicator = EventApplicator(self.storage)
        self.mock_event["action"] = 'BULKCONFIRM'
        self.set_contents({
            'confirms': [{
                '_entity_source_id': 'entity_source_id'
            }]
        })
        event = dict_to_object(self.mock_event)
        applicator.apply(event)
        self.assertEqual(len(applicator.add_events), 0)
