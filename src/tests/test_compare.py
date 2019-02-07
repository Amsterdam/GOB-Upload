import logging

from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.events import GOB
from gobcore.model import GOBModel
from gobupload.compare import compare, _shallow_compare
from gobupload.storage.handler import GOBStorageHandler
from tests import fixtures

@patch('gobupload.compare.GOBModel')
@patch('gobupload.compare.GOBStorageHandler')
class TestCompare(TestCase):
    def setUp(self):
        # Disable logging to prevent test from connecting to RabbitMQ
        logging.disable(logging.CRITICAL)
        self.mock_storage = MagicMock(spec=GOBStorageHandler)
        self.mock_model = MagicMock(spec=GOBModel)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_compare_fails_on_missing_dependencies(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage

        self.mock_storage.has_any_entity.return_value = False
        message = fixtures.get_message_fixture(contents=[])
        message["header"]["depends_on"] = {
            "xyz": "abc"
        }

        result = compare(message)
        self.assertEqual(result, None)

    def test_compare_succeeds_on_found_dependencies(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage

        # setup: one entity in db, none in message
        self.mock_storage.has_any_entity.return_value = True
        message = fixtures.get_message_fixture(contents=[])
        message["header"]["depends_on"] = {
            "xyz": "abc"
        }

        result = compare(message)
        self.assertNotEqual(result, None)

    def test_compare_creates_delete(self, storage_mock, model_mock,):
        storage_mock.return_value = self.mock_storage

        # setup: one entity in db, none in message
        self.mock_storage.compare_temporary_data.return_value = [{'_source_id': 1, '_entity_source_id': 1, 'type': 'DELETE', '_last_event': 1, '_hash': '1234567890'}]
        message = fixtures.get_message_fixture(contents=[])

        result = compare(message)

        # expectations: delete event is generated
        self.assertEqual(len(result['contents']['events']), 1)
        self.assertEqual(result['contents']['events'][0]['event'], 'DELETE')

    def test_compare_creates_add(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage

        # setup: no entity in db, one in message
        message = fixtures.get_message_fixture()
        data = message["contents"][0]
        self.mock_storage.compare_temporary_data.return_value = [{'_source_id': data['_source_id'], '_entity_source_id': data['_source_id'], 'type': 'ADD', '_last_event': 1, '_hash': '1234567890'}]

        result = compare(message)

        # expectations: add event is generated
        self.assertEqual(len(result['contents']['events']), 1)
        self.assertEqual(result['contents']['events'][0]['event'], 'ADD')

    def test_compare_creates_add_if_database_empty(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage

        # setup: no entity in db, one in message
        message = fixtures.get_message_fixture()
        self.mock_storage.has_any_entity.return_value = False

        result = compare(message)

        # expectations: add event is generated
        self.assertEqual(len(result['contents']['events']), 1)
        self.assertEqual(result['contents']['events'][0]['event'], 'ADD')

    def test_compare_creates_confirm(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage

        # setup: message and database have the same entity
        self.mock_storage.compare_temporary_data.return_value = [{'_source_id': 1, '_entity_source_id': 1, 'type': 'CONFIRM', '_last_event': 1, '_hash': '1234567890'}]
        message = fixtures.get_message_fixture()

        result = compare(message)

        # expectations: confirm event is generated
        self.assertEqual(len(result['contents']['events']), 1)
        self.assertEqual(result['contents']['events'][0]['event'], 'CONFIRM')

    def test_compare_creates_modify(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage
        model_mock.return_value = self.mock_model

        # setup: message and database have entity with same id but different data
        field_name = fixtures.random_string()
        old_value = fixtures.random_string()
        new_value = fixtures.random_string()

        # field moet ook in model!
        message = fixtures.get_message_fixture(contents=None, **{field_name: new_value})
        data_object = message['contents'][0]

        entity = fixtures.get_entity_fixture(**data_object)
        setattr(entity, field_name, old_value)

        self.mock_storage.get_current_ids.return_value = [entity]
        self.mock_storage.get_current_entity.return_value = entity

        # Add the field to the model as well
        self.mock_model.get_collection.return_value = {
            "fields": {
                field_name: {
                    "type": "GOB.String"
                }
            }
        }

        self.mock_storage.compare_temporary_data.return_value = [{'_source_id': data_object['_source_id'], '_entity_source_id': data_object['_source_id'], 'type': 'MODIFY', '_last_event': 1, '_hash': '1234567890'}]

        result = compare(message)

        # expectations: modify event is generated
        self.assertEqual(len(result['contents']['events']), 1)
        self.assertEqual(result['contents']['events'][0]['event'], 'MODIFY')

        # modificatinos dict has correct modifications.
        modifications = result['contents']['events'][0]['data']['modifications']
        self.assertEqual(len(modifications), 1)
        self.assertEqual(modifications[0]['key'], field_name)
        self.assertEqual(modifications[0]['old_value'], old_value)
        self.assertEqual(modifications[0]['new_value'], new_value)
