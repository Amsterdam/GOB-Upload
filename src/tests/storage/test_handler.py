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


class MockedEngine:

    def dispose(self):
        pass

    def execute(self, stmt):
        self.stmt = stmt

    def begin(self):
        return self

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass


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

        GOBStorageHandler.base = MagicMock()
        self.storage = GOBStorageHandler(metadata)
        GOBStorageHandler.engine = MagicMock()
        GOBStorageHandler.engine.__enter__ = lambda self: None
        GOBStorageHandler.engine.__exit__ = lambda *args: None
        GOBStorageHandler.engine.begin = lambda: GOBStorageHandler.engine

    @patch("gobupload.storage.handler.automap_base", MagicMock())
    def test_base(self):
        GOBStorageHandler.base = None
        GOBStorageHandler._set_base(update=True)
        self.assertIsNotNone(GOBStorageHandler.base)

    @patch("gobupload.storage.handler.alembic.config")
    @patch('gobupload.storage.handler.alembic.script')
    @patch("gobupload.storage.handler.migration")
    def test_init_storage(self, mock_alembic, mock_script, mock_config):
        context = MagicMock()
        context.get_current_revision.return_value = "revision 1"
        mock_alembic.MigrationContext.configure.return_value = context

        script = MagicMock()
        script.get_current_head.return_value = "revision 2"
        mock_script.ScriptDirectory.from_config.return_value = script

        self.storage._init_views = MagicMock()
        self.storage._get_reflected_base = MagicMock()
        self.storage._init_indexes = MagicMock()
        self.storage._set_base = MagicMock()

        self.storage.init_storage()
        # mock_alembic.config.main.assert_called_once()

        self.storage._init_views.assert_called_once()
        # self.storage._set_base.assert_called_with(update=True)
        self.storage._init_indexes.assert_called_once()


    def test_init_indexes(self):
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
            },
            "json_index": {
                "table_name": "table_with_json",
                "columns": ["somejsoncol"],
                "type": "json",
            },
        }

        self.storage._init_indexes()
        self.storage.engine.execute.assert_has_calls([
            call("CREATE INDEX IF NOT EXISTS \"indexname\" ON sometable USING BTREE(cola,colb)"),
            call().close(),
            call("CREATE INDEX IF NOT EXISTS \"index2name\" ON someothertable USING BTREE(cola)"),
            call().close(),
            call("CREATE INDEX IF NOT EXISTS \"geo_index\" ON table_with_geo USING GIST(geocol)"),
            call().close(),
            call("CREATE INDEX IF NOT EXISTS \"json_index\" ON table_with_json USING GIN(somejsoncol)"),
            call().close(),
        ])


    def test_create_temporary_table(self):
        expected_table = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}_tmp'

        self.storage.create_temporary_table()

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

        # Assert the drop function is called
        self.storage.engine.execute.assert_any_call(f"DROP TABLE IF EXISTS {expected_table}")

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

    def test_delete_confirms(self):
        catalogue = self.msg["header"]["catalogue"]
        entity = self.msg["header"]["entity"]
        events = self.storage.EVENTS_TABLE
        self.storage.delete_confirms()

        self.storage.engine.execute.assert_called()
        args = self.storage.engine.execute.call_args[0][0]
        args = ' '.join(args.split())
        expect = f"DELETE FROM {events} WHERE catalogue = '{catalogue}' AND entity = '{entity}' AND action IN ('BULKCONFIRM', 'CONFIRM')"
        self.assertEqual(args, expect)

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

    def test_combinations_plain(self):
        mock_session = MagicMock()
        self.storage.get_session = mock_session
        result = self.storage.get_source_catalogue_entity_combinations()
        mock_session.return_value.__enter__().execute.assert_called_with('SELECT DISTINCT source, catalogue, entity FROM events')

    def test_combinations_with_args(self):
        mock_session = MagicMock()
        self.storage.get_session = mock_session
        result = self.storage.get_source_catalogue_entity_combinations(col="val")
        mock_session.return_value.__enter__().execute.assert_called_with("SELECT DISTINCT source, catalogue, entity FROM events WHERE col = 'val'")
