import logging

from unittest import TestCase, mock
from unittest.mock import MagicMock, patch, ANY
from tests import fixtures

from gobcore.message_broker.offline_contents import ContentsWriter

from gobupload.compare.main import compare, GOBStorageHandler, GOBModel
from gobupload.compare.event_collector import EventCollector

mock_model = MagicMock(spec=GOBModel)
mock_writer = MagicMock(spec=ContentsWriter)
mock_event_collector = MagicMock(spec=EventCollector)


@patch('gobupload.compare.main.ContentsWriter', mock_writer)
@patch('gobupload.compare.main.GOBModel')
@patch('gobupload.compare.main.GOBStorageHandler')
class TestCompare(TestCase):

    def setUp(self):
        # Disable logging to prevent test from connecting to RabbitMQ
        logging.disable(logging.CRITICAL)
        self.mock_storage = MagicMock(spec=GOBStorageHandler)
        mock_model.get_collection.return_value = {
            "entity_id": "identificatie",
            "version": '0.9',
            "has_states": False,
        }
        mock_event_collector.reset_mock()
        mock_writer.reset_mock()

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_compare_fails_on_missing_dependencies(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage

        self.mock_storage.has_any_event.return_value = False
        message = fixtures.get_message_fixture(contents=[])
        message["header"]["depends_on"] = {
            "xyz": "abc"
        }

        result = compare(message)
        self.assertEqual(result, {'header': mock.ANY, 'summary': mock.ANY, 'contents': None})

    def test_compare_succeeds_on_found_dependencies(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage

        # setup: one entity in db, none in message
        self.mock_storage.has_any_event.return_value = True
        message = fixtures.get_message_fixture(contents=[])
        message["header"]["depends_on"] = {
            "xyz": "abc"
        }

        result = compare(message)
        self.assertNotEqual(result, None)

    def test_compare_creates_delete(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage
        model_mock.return_value = mock_model

        # setup: message and database have the same entity
        original_value = {
            "_last_event": 123
        }
        self.mock_storage.compare_temporary_data.return_value = [
            {'_original_value': original_value, '_tid': 1, 'type': 'DELETE',
             '_entity_tid': 2, '_last_event': 1, '_hash': '1234567890'}]
        message = fixtures.get_message_fixture()

        result = compare(message)

        # expectations: confirm event is generated
        self.assertIsNotNone(result["contents_ref"])
        mock_writer.return_value.__enter__().write.assert_called_once()
        mock_writer.return_value.__enter__().write.assert_called_with(
            {'event': 'DELETE', 'data': ANY, 'version': '0.9'})

    def test_compare_creates_add(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage
        model_mock.return_value = mock_model

        # setup: no entity in db, one in message
        message = fixtures.get_message_fixture()
        data = message["contents"][0]
        self.mock_storage.has_any_event.return_value = True
        original_value = {
            "_last_event": 123
        }
        self.mock_storage.compare_temporary_data.return_value = [
            {'_original_value': original_value, '_tid': data['_tid'],
             'type': 'ADD', '_last_event': 1, '_hash': '1234567890'}]

        result = compare(message)

        # expectations: add event is generated
        self.assertIsNotNone(result["contents_ref"])
        mock_writer.return_value.__enter__().write.assert_called_once()
        mock_writer.return_value.__enter__().write.assert_called_with({'event': 'ADD', 'data': ANY, 'version': '0.9'})

    @patch('gobupload.compare.main.EventCollector', mock_event_collector)
    def test_initial_add(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage
        model_mock.return_value = mock_model

        # setup: no entity in db, one in message
        message = fixtures.get_message_fixture()
        data = message["contents"][0]
        self.mock_storage.has_any_event.return_value = True
        self.mock_storage.has_any_entity.return_value = False
        original_value = {
            "_last_event": 123
        }
        self.mock_storage.compare_temporary_data.return_value = [
            {'_original_value': original_value, '_tid': data['_tid'],
             'type': 'ADD', '_last_event': 1, '_hash': '1234567890'}]

        result = compare(message)

        # expectations: add event is generated
        self.assertIsNotNone(result["contents_ref"])
        mock_writer.return_value.__enter__().write.assert_not_called()
        mock_event_collector.return_value.collect_initial_add.assert_called_once()

    def test_compare_creates_confirm(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage
        model_mock.return_value = mock_model

        # setup: message and database have the same entity
        original_value = {
            "_last_event": 123
        }
        self.mock_storage.compare_temporary_data.return_value = [
            {'_original_value': original_value, '_tid': 1, 'type': 'CONFIRM',
             '_last_event': 1, '_hash': '1234567890'}]
        message = fixtures.get_message_fixture()

        result = compare(message)

        # expectations: confirm event is generated
        self.assertIsNotNone(result["contents_ref"])
        mock_writer.return_value.__enter__().write.assert_called_once()
        mock_writer.return_value.__enter__().write.assert_called_with(
            {'event': 'CONFIRM', 'data': ANY, 'version': '0.9'})

    def test_compare_creates_bulkconfirm(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage
        model_mock.return_value = mock_model

        # setup: message and database have the same entities
        original_value = {
            "_last_event": 123
        }
        self.mock_storage.compare_temporary_data.return_value = [
            {'_original_value': original_value, '_tid': 1, 'type': 'CONFIRM',
             '_last_event': 1, '_hash': '1234567890'},
            {'_original_value': original_value, '_tid': 1, 'type': 'CONFIRM',
             '_last_event': 1, '_hash': '1234567890'}
        ]
        message = fixtures.get_message_fixture()

        result = compare(message)

        # expectations: confirm event is generated
        self.assertIsNotNone(result["contents_ref"])
        mock_writer.return_value.__enter__().write.assert_called_once()
        mock_writer.return_value.__enter__().write.assert_called_with(
            {'event': 'BULKCONFIRM', 'data': ANY, 'version': '0.9'})

    def test_compare_creates_modify(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage
        model_mock.return_value = mock_model

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
        mock_model.get_collection.return_value = {
            "entity_id": "identificatie",
            "version": '0.9',
            "has_states": False,
            "all_fields": {
                field_name: {
                    "type": "GOB.String"
                }
            }
        }

        original_value = {
            "_last_event": 123,
            "_tid": data_object['_tid'],
            "_hash": "1234",
            field_name: new_value
        }
        self.mock_storage.compare_temporary_data.return_value = [
            {'_original_value': original_value, '_tid': data_object['_tid'],
             'type': 'MODIFY', '_last_event': 1,
             '_hash': '1234567890'}]

        result = compare(message)

        # expectations: modify event is generated
        self.assertIsNotNone(result["contents_ref"])
        mock_writer.return_value.__enter__().write.assert_called_once()
        mock_writer.return_value.__enter__().write.assert_called_with(
            {'event': 'MODIFY', 'data': ANY, 'version': '0.9'})

        result = mock_writer.return_value.__enter__().write.call_args_list[0][0][0]

        # modificatinos dict has correct modifications.
        modifications = result['data']['modifications']
        self.assertEqual(len(modifications), 1)
        self.assertEqual(modifications[0]['key'], field_name)
        self.assertEqual(modifications[0]['old_value'], old_value)
        self.assertEqual(modifications[0]['new_value'], new_value)
