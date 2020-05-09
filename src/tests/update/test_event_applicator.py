import json

from unittest import TestCase
from unittest.mock import patch, MagicMock

from tests.fixtures import dict_to_object

from gobcore.exceptions import GOBException
from gobcore.events import GOB
from gobupload.storage.handler import GOBStorageHandler
from gobupload.update.event_applicator import EventApplicator, _get_gob_event


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
        self.assertEqual(len(applicator.other_events), 1)

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

    @patch('gobupload.update.event_applicator.GOBModel')
    @patch('gobupload.update.event_applicator.GobEvent')
    @patch('gobupload.update.event_applicator.MessageMetaData')
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

    @patch('gobupload.update.event_applicator.GOBMigrations')
    @patch('gobupload.update.event_applicator.GOBModel')
    @patch('gobupload.update.event_applicator.GobEvent')
    @patch('gobupload.update.event_applicator.MessageMetaData')
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

    def test_add_other_event(self):
        applicator = EventApplicator(self.storage, {})

        applicator.MAX_OTHER_CHUNK = 3
        applicator.apply_other_events = MagicMock()

        applicator.add_other_event('any gob event', {'_entity_source_id': 'any entity source 1'})
        self.assertEqual(applicator.other_events['any entity source 1'], 'any gob event')

        applicator.add_other_event('any gob event', {'_entity_source_id': 'any entity source 2'})
        applicator.apply_other_events.assert_not_called()

        applicator.add_other_event('any gob event', {'_entity_source_id': 'any entity source 3'})
        applicator.apply_other_events.assert_called()

    def test_apply_other_events(self):
        applicator = EventApplicator(self.storage, {})
        applicator.apply_other_event = MagicMock()

        self.assertEqual(applicator.other_events, {})

        applicator.apply_other_events()
        self.assertEqual(applicator.other_events, {})
        self.storage.get_entities.assert_not_called()

        applicator.add_other_event('any gob event', {'_entity_source_id': 'any entity source id'})
        self.storage.get_entities.return_value = ['any entity']

        applicator.apply_other_events()
        self.assertEqual(applicator.other_events, {})
        self.storage.get_entities.assert_called()
        applicator.apply_other_event.assert_called_with('any entity')

    def test_apply_other_event(self):
        applicator = EventApplicator(self.storage, {})

        entity = MagicMock()
        entity._date_deleted = None
        entity._last_event = None

        gob_event = MagicMock()
        gob_event.id = 'any event id'

        entity._source_id = 'any source id'
        applicator.other_events['any source id'] = gob_event

        # Normal action, apply event and set last event id
        applicator.apply_other_event(entity)
        gob_event.apply_to.assert_called_with(entity)
        self.assertEqual(entity._last_event, gob_event.id)

        # Apply NON-ADD event on a deleted entity
        entity._date_deleted = 'any date deleted'
        with self.assertRaises(GOBException):
            applicator.apply_other_event(entity)

        # Apply ADD event on a deleted entity is OK
        gob_event = MagicMock(spec=GOB.ADD)
        gob_event.id = 'any event id'
        entity._last_event = None
        applicator.other_events['any source id'] = gob_event
        applicator.apply_other_event(entity)
        self.assertEqual(entity._last_event, gob_event.id)

        # Do not set last event id for CONFIRM events
        gob_event = MagicMock(spec=GOB.CONFIRM)
        gob_event.id = 'any event id'
        entity._last_event = None
        entity._date_deleted = None
        applicator.other_events['any source id'] = gob_event
        applicator.apply_other_event(entity)
        self.assertEqual(entity._last_event, None)
