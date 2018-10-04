from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.model import GOBModel
from gobuploadservice.compare import compare
from gobuploadservice.storage.handler import GOBStorageHandler
from tests import fixtures


@patch('gobuploadservice.compare.GOBModel')
@patch('gobuploadservice.compare.GOBStorageHandler')
class TestCompare(TestCase):
    def setUp(self):
        self.mock_storage = MagicMock(spec=GOBStorageHandler)
        self.mock_model = MagicMock(spec=GOBModel)

    def test_compare_creates_delete(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage

        # setup: one entity in db, none in message
        self.mock_storage.get_current_ids.return_value = [fixtures.get_entity_fixture(_source_id=1)]
        message = fixtures.get_message_fixture(contents=[])

        result = compare(message)

        # expectations: delete event is generated
        self.assertEqual(len(result['contents']), 1)
        self.assertEqual(result['contents'][0]['event'], 'DELETE')

    def test_compare_creates_add(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage

        # setup: no entity in db, one in message
        self.mock_storage.get_entity_or_none.return_value = None
        message = fixtures.get_message_fixture()

        result = compare(message)

        # expectations: add event is generated
        self.assertEqual(len(result['contents']), 1)
        self.assertEqual(result['contents'][0]['event'], 'ADD')

    def test_compare_creates_confirm(self, storage_mock, model_mock):
        storage_mock.return_value = self.mock_storage

        # setup: message and database have the same entity
        message = fixtures.get_message_fixture()
        data_object = message['contents'][0]

        entity = fixtures.get_entity_fixture(**data_object)

        self.mock_storage.get_current_ids.return_value = [entity]
        self.mock_storage.get_entity_or_none.return_value = entity

        result = compare(message)

        # expectations: confirm event is generated
        self.assertEqual(len(result['contents']), 1)
        self.assertEqual(result['contents'][0]['event'], 'CONFIRM')

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
        self.mock_storage.get_entity_or_none.return_value = entity

        # Add the field to the model as well
        self.mock_model.get_model.return_value = {
            "fields": {
                field_name: {
                    "type": "GOB.String"
                }
            }
        }

        result = compare(message)

        # expectations: modify event is generated
        self.assertEqual(len(result['contents']), 1)
        self.assertEqual(result['contents'][0]['event'], 'MODIFY')

        # modificatinos dict has correct modifications.
        modifications = result['contents'][0]['data']['modifications']
        self.assertEqual(len(modifications), 1)
        self.assertEqual(modifications[0]['key'], field_name)
        self.assertEqual(modifications[0]['old_value'], old_value)
        self.assertEqual(modifications[0]['new_value'], new_value)
