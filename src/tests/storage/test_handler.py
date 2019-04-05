import importlib
import unittest
from unittest.mock import call, MagicMock, patch, Mock

from gobcore.events.import_message import ImportMessage
from gobcore.exceptions import GOBException
from gobcore.model import GOBModel

from gobupload.compare import populate
from gobupload.storage import queries
from gobupload.storage.handler import GOBStorageHandler
from tests import fixtures


class TestStorageHandler(unittest.TestCase):

    @patch('gobupload.storage.handler.create_engine', MagicMock())
    def setUp(self):
        self.mock_model = MagicMock(spec=GOBModel)
        self.msg = fixtures.get_message_fixture()
        model = {
            "entity_id": "identificatie",
            "version": "1"
        }
        # Add the hash to the message
        populate(self.msg, model)

        message = ImportMessage(self.msg)
        metadata = message.metadata

        self.storage = GOBStorageHandler(metadata)

    def test_create_temporary_table(self):
        expected_table = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}_tmp'

        self.storage.create_temporary_table()

        # Assert the test table has been made
        self.assertIn(expected_table, self.storage.base.metadata.tables)

        for entity in self.msg["contents"]:
            self.storage.write_temporary_entity(entity)

        # And the engine has been called to fill the temporary table
        self.storage.engine.execute.assert_called()

    def test_create_temporary_table_exists(self):
        expected_table = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}_tmp'

        mock_table = MagicMock()

        # Make sure the test table already exists
        self.storage.base.metadata.tables = {expected_table: mock_table}
        self.storage.create_temporary_table()

        # Assert the truncate function is called
        self.storage.engine.execute.assert_any_call(f"TRUNCATE {expected_table}")

        for entity in self.msg["contents"]:
            self.storage.write_temporary_entity(entity)

        # And the engine has been called to fill the temporary table
        self.storage.engine.execute.assert_called()

    def test_compare_temporary_data(self, mock_get_comparison):
        current_table = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}'
        new_table = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}_tmp'

        self.storage.compare_temporary_data()

        # Check if the get comparison function is called for confirms and changes
        mock_get_comparison.assert_any_call(current_table, new_table)
        mock_get_comparison.assert_any_call(current_table, new_table, False)

        # Assert the temporary table is deleted
        self.storage.engine.execute.assert_any_call(f"DROP TABLE {new_table}")

    def test_compare_temporary_data(self):
        current = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}'
        temporary = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}_tmp'

        fields = ['_source', 'identificatie']
        query = queries.get_comparison_query(current, temporary, fields)

        self.storage.compare_temporary_data()

        # Assert the query is performed is deleted
        self.storage.engine.execute.assert_any_call(query)

        # Assert the temporary table is deleted
        self.storage.engine.execute.assert_any_call(f"DROP TABLE IF EXISTS {temporary}")

    def test_bulk_add_events(self):
        metadata = fixtures.get_metadata_fixture()
        event = fixtures.get_event_fixture(metadata)

        # Make sure the events table exists
        self.storage.base.metadata.tables = {'events': MagicMock()}
        self.storage.bulk_insert = MagicMock()

        self.storage.bulk_add_events([event])
        # Assert the query is performed
        self.storage.bulk_insert.assert_called()

    def test_bulk_insert(self):
        insert_data = {'key': 'value'}
        table = MagicMock()

        self.storage.bulk_insert(table, insert_data)
        # Assert the query is performed
        self.storage.engine.execute.assert_called()

    def test_get_entity_for_update_modify_non_existing_entity(self):
        event = fixtures.get_event_fixure()
        event.action = 'MODIFY'
        data = {
            '_entity_source_id': fixtures.random_string()
        }
        self.storage.get_entity_or_none = MagicMock(return_value=None)
        with self.assertRaises(GOBException):
            self.storage.get_entity_for_update(event, data)

    def test_get_entity_for_update_modify_deleted_entity(self):
        entity = fixtures.get_entity_fixture(**{'_date_deleted': 'value'})
        event = fixtures.get_event_fixure()
        event.action = 'MODIFY'
        data = {
            '_entity_source_id': fixtures.random_string()
        }
        self.storage.get_entity_or_none = MagicMock(return_value=entity)
        with self.assertRaises(GOBException):
            self.storage.get_entity_for_update(event, data)

    def test_get_query_value(self):
        self.storage.get_query_value('SELECT * FROM test')
        # Assert the query is performed
        self.storage.engine.execute.assert_called_with('SELECT * FROM test')
