import importlib
import unittest
from unittest.mock import call, MagicMock, patch, Mock

from gobcore.events.import_message import ImportMessage
from gobcore.exceptions import GOBException
from gobcore.model import GOBModel

import gobupload.storage.handler
from gobupload.compare.populate import Populator
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
        populator = Populator(model, self.msg)
        for content in self.msg['contents']:
            populator.populate(content)

        message = ImportMessage(self.msg)
        metadata = message.metadata

        self.storage = GOBStorageHandler(metadata)

    @patch("gobupload.storage.handler.alembic")
    def test_init_storage(self, mock_alembic):
        self.storage._init_views = MagicMock()
        self.storage._get_reflected_base = MagicMock()
        self.storage._init_indexes = MagicMock()

        self.storage.init_storage()
        mock_alembic.config.main.assert_called_once()

        self.storage._init_views.assert_called_once()
        self.storage._get_reflected_base.assert_called_once()
        self.storage._init_indexes.assert_called_once()


    def test_init_indexes(self):
        self.storage.engine = MagicMock()
        gobupload.storage.handler.indexes = {
            "indexname": {
                "table_name": "sometable",
                "columns": ["cola", "colb"],
            },
            "index2name": {
                "table_name": "someothertable",
                "columns": ["cola"],
            },
            "geo_index": {
                "table_name": "table_with_geo",
                "columns": ["geocol"],
                "type": "geo",
            }
        }

        self.storage._init_indexes()
        self.storage.engine.execute.assert_has_calls([
            call("CREATE INDEX IF NOT EXISTS \"indexname\" ON sometable (cola,colb)"),
            call("CREATE INDEX IF NOT EXISTS \"index2name\" ON someothertable (cola)"),
            call("CREATE INDEX IF NOT EXISTS \"geo_index\" ON table_with_geo USING GIST(geocol)"),
        ])


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

        diff = self.storage.compare_temporary_data()
        results = [result for result in diff]

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

        diff = self.storage.compare_temporary_data()
        results = [result for result in diff]

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
