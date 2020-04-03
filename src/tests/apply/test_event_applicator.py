import json

from unittest import TestCase
from unittest.mock import patch, MagicMock

from tests.fixtures import dict_to_object

from gobupload.storage.handler import GOBStorageHandler
from gobupload.apply.event_applicator import EventApplicator, _get_gob_event


class TestEventApplicator(TestCase):

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
        applicator = EventApplicator(self.storage, {})
        self.assertEqual(applicator.add_events, [])

    def test_apply(self):
        applicator = EventApplicator(self.storage, {})
        self.mock_event["action"] = 'CONFIRM'
        self.set_contents({
            '_entity_source_id': 'entity_source_id',
            '_hash': '123'
        })
        event = dict_to_object(self.mock_event)
        applicator.apply(event)
        self.assertEqual(len(applicator.add_events), 0)
        self.storage.get_entity_for_update.assert_called()

    def test_apply_new_add(self):
        self.set_contents({
            '_entity_source_id': 'entity_source_id',
            '_hash': '123'
        })
        event = dict_to_object(self.mock_event)
        with EventApplicator(self.storage, {}) as applicator:
            applicator.apply(event)
            self.assertEqual(len(applicator.add_events), 1)
        self.assertEqual(len(applicator.add_events), 0)
        self.storage.get_entity_for_update.assert_not_called()
        self.storage.add_add_events.assert_called()

    def test_apply_bulk(self):
        applicator = EventApplicator(self.storage, {})
        self.mock_event["action"] = 'BULKCONFIRM'
        self.set_contents({
            'confirms': [{
                '_entity_source_id': 'entity_source_id'
            }]
        })
        event = dict_to_object(self.mock_event)
        applicator.apply(event)
        self.assertEqual(len(applicator.add_events), 0)
        self.storage.bulk_update_confirms.assert_called()

    @patch('gobupload.apply.event_applicator.GOBModel')
    @patch('gobupload.apply.event_applicator.GobEvent')
    @patch('gobupload.apply.event_applicator.MessageMetaData')
    def test_get_gob_event(self, mock_message_meta_data, mock_gob_event, mock_model):
        mock_model().get_collection.return_value = {
            'version': '0.1'
        }

        event = dict_to_object(self.mock_event)
        data = {
            'entity': {
                '_version': '0.1'
            }
        }

        expected_event_msg = {
            'event': event.action,
            'data': data
        }
        expected_meta_data = mock_message_meta_data.return_value = 'meta_data'

        _get_gob_event(event, data)

        mock_gob_event.assert_called_with(expected_event_msg, expected_meta_data)

    @patch('gobupload.apply.event_applicator.GOBMigrations')
    @patch('gobupload.apply.event_applicator.GOBModel')
    @patch('gobupload.apply.event_applicator.GobEvent')
    @patch('gobupload.apply.event_applicator.MessageMetaData')
    def test_get_gob_event_migration(self, mock_message_meta_data, mock_gob_event, mock_model, mock_migrations):
        target_version = '0.2'

        mock_model().get_collection.return_value = {
            'version': target_version
        }

        mock_migrations().migrate_event_data.return_value = {
            'entity': {
                '_version': target_version
            }
        }

        event = dict_to_object(self.mock_event)
        data = {
            'entity': {
                '_version': '0.1'
            }
        }

        expected_event_msg = {
            'event': event.action,
            'data': {
                'entity': {
                    '_version': target_version
                }
            }
        }
        expected_meta_data = mock_message_meta_data.return_value = 'meta_data'

        _get_gob_event(event, data)

        mock_migrations().migrate_event_data.assert_called_with(event, data, event.catalogue, event.entity, target_version)

        mock_gob_event.assert_called_with(expected_event_msg, expected_meta_data)
