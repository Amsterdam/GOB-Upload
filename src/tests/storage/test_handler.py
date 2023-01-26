import datetime
import unittest
from decimal import Decimal
from unittest.mock import call, MagicMock, patch

from sqlalchemy import Integer, DateTime, String, JSON
from sqlalchemy.orm import declarative_base

from gobcore.events.import_message import ImportMessage
from gobcore.exceptions import GOBException

from sqlalchemy.exc import OperationalError
import sqlalchemy as sa

from gobupload.compare.populate import Populator
from gobupload.storage import queries
from gobupload.storage.handler import GOBStorageHandler, StreamSession
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


Base = declarative_base()


class MockEvents(Base):
    __tablename__ = "events"

    eventid = sa.Column(Integer, primary_key=True)
    timestamp = sa.Column(DateTime)
    catalogue = sa.Column(String)
    entity = sa.Column(String)
    version = sa.Column(String)
    action = sa.Column(String)
    source = sa.Column(String)
    source_id = sa.Column(String)
    contents = sa.Column(JSON)
    application = sa.Column(String)
    tid = sa.Column(String)


class MockMeta:
    source = "AMSBI"
    catalogue = "meetbouten"
    entity = "meetbouten"


class TestStorageHandler(unittest.TestCase):

    @patch("gobupload.storage.handler.automap_base", MagicMock())
    @patch('gobupload.storage.handler.create_engine', MagicMock())
    def setUp(self):
        self.msg = fixtures.get_message_fixture()
        model = {
            "entity_id": "identificatie",
            "version": "1",
            "has_states": False,
        }
        # Add the hash to the message
        populator = Populator(model, self.msg)
        self.msg['header']['source'] = 'any source'
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

        GOBStorageHandler.base.classes.events = MockEvents

    @patch("gobupload.storage.handler.automap_base")
    def test_base(self, mock_base):
        # automap called
        GOBStorageHandler.base = None
        GOBStorageHandler._set_base()
        assert GOBStorageHandler.base is not None
        mock_base.return_value.prepare.assert_called_with(autoload_with=GOBStorageHandler.engine)

        # automap is not called, base unchanged
        mock_base.reset_mock()
        GOBStorageHandler.base = "i_am_base"
        GOBStorageHandler._set_base()
        assert GOBStorageHandler.base == "i_am_base"
        mock_base.return_value.prepare.assert_not_called()

        # automap is called, update = True
        mock_base.reset_mock()
        GOBStorageHandler.base = None
        GOBStorageHandler._set_base(update=True)
        assert GOBStorageHandler.base is not None
        mock_base.return_value.prepare.assert_called_with(autoload_with=GOBStorageHandler.engine)

        # automap is called, base is None
        mock_base.reset_mock()
        GOBStorageHandler.base = None
        GOBStorageHandler._set_base(update=False)
        assert GOBStorageHandler.base is not None
        mock_base.return_value.prepare.assert_called_with(autoload_with=GOBStorageHandler.engine)

        # reflection options, base is None
        mock_base.reset_mock()
        GOBStorageHandler.base = None
        GOBStorageHandler._set_base(reflection_options={"opt1": "val1"})
        mock_base.return_value.prepare.assert_called_with(
            autoload_with=GOBStorageHandler.engine,
            reflection_options={"opt1": "val1"}
        )

        # reflection options, base exists
        mock_base.reset_mock()
        GOBStorageHandler.base = "i_am_base"
        GOBStorageHandler._set_base(reflection_options={"opt1": "val1"})
        mock_base.return_value.prepare.assert_called_with(
            autoload_with=GOBStorageHandler.engine,
            reflection_options={"opt1": "val1"}
        )

    @patch("gobupload.storage.handler.GOBStorageHandler._set_base")
    def test_init(self, mock_set_base):
        storage = GOBStorageHandler()
        assert storage.metadata is None
        mock_set_base.assert_called_with(reflection_options={})

        # only param
        storage = GOBStorageHandler(only=["table2"])
        assert storage.metadata is None
        mock_set_base.assert_called_with(reflection_options={"only": ["table2"]})

        # metadata
        storage = GOBStorageHandler(gob_metadata=MockMeta)
        assert storage.metadata == MockMeta
        mock_set_base.assert_called_with(reflection_options={"only": ["events", "meetbouten_meetbouten"]})

        # metadata + only
        storage = GOBStorageHandler(gob_metadata=MockMeta, only=["table2"])
        assert storage.metadata == MockMeta
        mock_set_base.assert_called_with(reflection_options={"only": ["events", "meetbouten_meetbouten", "table2"]})

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

        self.storage._get_reflected_base = MagicMock()
        self.storage._init_indexes = MagicMock()
        self.storage._set_base = MagicMock()
        self.storage._init_relation_materialized_views = MagicMock()
        self.storage._check_configuration = MagicMock()

        # no error
        self.storage.init_storage(recreate_materialized_views='booleanValue')
        # mock_alembic.config.main.assert_called_once()

        # self.storage._set_base.assert_called_with(update=True)
        self.storage._init_indexes.assert_called_once()
        self.storage._init_relation_materialized_views.assert_called_with('booleanValue')
        self.storage._check_configuration.assert_called_once()

        # raise error during migration
        mock_config.main.side_effect = Exception("My error")
        GOBStorageHandler.engine.execute.reset_mock()

        with self.assertRaisesRegex(Exception, "My error"):
            self.storage.init_storage(force_migrate=False)

        # assert we are unlocking after exception
        GOBStorageHandler.engine.execute.has_calls([
            call("SELECT pg_advisory_lock(19935910)"),
            call("SELECT pg_advisory_unlock(19935910)")
        ])

    def test_get_config_value(self):
        self.storage.engine = MagicMock()
        self.storage.engine.execute.return_value = iter([('the value',)])

        self.assertEqual('the value', self.storage._get_config_value('the setting'))
        self.storage.engine.execute.assert_called_with('SHOW the setting')

    @patch("builtins.print")
    def test_check_configuration(self, mock_print):
        self.storage._get_config_value = lambda x: 'the value'
        self.storage.config_checks = [
            ('the setting', lambda x: True, 'the message', self.storage.WARNING)
        ]

        self.storage._check_configuration()
        mock_print.assert_not_called()

        self.storage.config_checks = [
            ('the setting', lambda x: False, 'the message', self.storage.WARNING)
        ]

        self.storage._check_configuration()
        mock_print.assert_called_with('WARNING: Checking Postgres config for the setting. '
                                      'Value is the value, but the message')
        mock_print.reset_mock()

        self.storage.config_checks = [
            ('the setting', lambda x: False, 'the message', self.storage.ERROR)
        ]

        with self.assertRaises(GOBException):
            self.storage._check_configuration()

    @patch("gobupload.storage.handler.MaterializedViews")
    def test_init_relation_materialized_view(self, mock_materialized_views):
        self.storage._init_relation_materialized_views()

        mock_materialized_views.assert_called_once()
        mock_materialized_views.return_value.initialise.assert_called_with(self.storage, False)

        mock_materialized_views.reset_mock()
        self.storage._init_relation_materialized_views(True)

        mock_materialized_views.assert_called_once()
        mock_materialized_views.return_value.initialise.assert_called_with(self.storage, True)

    def test_indexes_to_drop_query(self):
        expected = """
SELECT
    s.indexrelname
FROM pg_catalog.pg_stat_user_indexes s
JOIN pg_catalog.pg_index i ON s.indexrelid = i.indexrelid
WHERE
    s.relname in ('sometable_1','sometable_2')
    AND s.indexrelname not in ('index_a','index_b')
    AND 0 <> ALL (i.indkey)    -- no index column is an expression
    AND NOT i.indisunique  -- no unique indexes
    AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint c WHERE c.conindid = s.indexrelid)
"""
        self.assertEqual(expected, self.storage._indexes_to_drop_query(
            ['sometable_1', 'sometable_2'],
            ['index_a', 'index_b']
        ))

    def test_drop_indexes(self):
        indexes = {
            "index_a": {
                "table_name": "sometable_1",
                "columns": ["col_a", "col_b"],
            },
            "index_b": {
                "table_name": "sometable_2",
                "columns": ["col_a", "col_b"],
            },
        }

        self.storage.engine = MagicMock()
        self.storage._indexes_to_drop_query = MagicMock()
        self.storage.engine.execute.return_value = [("index_c",), ("index_d",)]
        self.storage.execute = MagicMock()

        self.storage._drop_indexes(indexes)

        self.storage.execute.assert_has_calls([
            call('DROP INDEX IF EXISTS "index_c"'),
            call('DROP INDEX IF EXISTS "index_d"'),
        ])

        self.storage._indexes_to_drop_query.assert_called_with(
            ['sometable_1', 'sometable_2'],
            ['index_a', 'index_b'],
        )
        self.storage.engine.execute.assert_called_with(self.storage._indexes_to_drop_query.return_value)

    @patch("builtins.print")
    def test_drop_indexes_exception_get(self, mock_print):
        e = OperationalError('stmt', 'params', 'orig')
        self.storage.engine.execute = MagicMock(side_effect=e)
        self.storage._indexes_to_drop_query = MagicMock()
        self.storage._drop_indexes({})

        mock_print.assert_called_with(f"ERROR: Could not get indexes to drop: {str(e)}")

    @patch("builtins.print")
    def test_drop_indexes_exception_drop(self, mock_print):
        e = OperationalError('stmt', 'params', 'orig')
        self.storage.engine.execute = MagicMock(return_value=[('a',)])
        self.storage._indexes_to_drop_query = MagicMock()
        self.storage.execute = MagicMock(side_effect=e)
        self.storage._drop_indexes({})

        mock_print.assert_called_with(f"ERROR: Could not drop index a: {str(e)}")

    def test_get_existing_indexes(self):
        self.storage.engine.execute = MagicMock(return_value=[('indexA',), ('indexB',)])
        self.assertEqual(['indexA', 'indexB'], self.storage._get_existing_indexes())

    @patch("builtins.print")
    def test_get_existing_indexes_exception(self, mock_print):
        e = OperationalError('stmt', 'params', 'orig')
        self.storage.engine.execute = MagicMock(side_effect=e)

        self.assertEqual([], self.storage._get_existing_indexes())
        mock_print.assert_called_with(f"WARNING: Could not fetch list of existing indexes: {e}")

    @patch('gobupload.storage.handler.get_indexes')
    def test_init_indexes(self, mock_get_indexes):
        mock_get_indexes.return_value = {
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
            "existing": {

            }
        }

        self.storage._drop_indexes = MagicMock()
        self.storage._get_existing_indexes = lambda: ['existing']
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
        self.storage._drop_indexes.assert_called_once()

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
        expected_table = self.storage.create_temporary_table()

        # Assert the drop function is called
        self.storage.engine.execute.assert_any_call(f"DROP TABLE IF EXISTS {expected_table}")

        for entity in self.msg["contents"]:
            self.storage.write_temporary_entity(entity)

        # And the engine has been called to fill the temporary table
        self.storage.engine.execute.assert_called()

    def test_compare_temporary_data(self):
        current = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}'
        temporary = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}_tmp'

        fields = ['_tid']
        query = queries.get_comparison_query('any source', current, temporary, fields)

        diff = self.storage.compare_temporary_data(temporary)
        results = [result for result in diff]

        self.storage.engine.execution_options.assert_called_with(stream_results=True)

        # Assert the query is performed is deleted
        self.storage.engine.execution_options().execute.assert_any_call(query)

        # Assert the temporary table is deleted
        self.storage.engine.execute.assert_any_call(f"DROP TABLE IF EXISTS {temporary}")

    def test_get_query_value(self):
        self.storage.get_query_value('SELECT * FROM test')
        # Assert the query is performed
        self.storage.engine.execute.assert_called_with('SELECT * FROM test')

    def test_combinations_plain(self):
        mock_session = MagicMock()
        self.storage.get_session = mock_session

        result = self.storage.get_source_catalogue_entity_combinations()
        self.assertIsInstance(result, list)

        query = mock_session.return_value.__enter__().stream_execute.call_args[0][0]
        query = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertEqual(
            "SELECT DISTINCT events.source, events.catalogue, events.entity \n"
            "FROM events",
            query
        )

    def test_combinations_with_args(self):
        mock_session = MagicMock()
        self.storage.get_session = mock_session

        result = self.storage.get_source_catalogue_entity_combinations(source="val")
        self.assertIsInstance(result, list)

        query = mock_session.return_value.__enter__().stream_execute.call_args[0][0]
        query = str(query.compile(compile_kwargs={"literal_binds": True}))
        self.assertEqual(
            "SELECT DISTINCT events.source, events.catalogue, events.entity \n"
            "FROM events \n"
            "WHERE events.source = 'val'",
            query
        )

    @patch('gobupload.storage.handler.gob_model.get_table_name')
    def test_get_tablename(self, mock_get_table_name):
        result = self.storage._get_tablename()
        self.assertEqual(mock_get_table_name.return_value, result)
        mock_get_table_name.assert_called_with(
            self.storage.metadata.catalogue, self.storage.metadata.entity)

    def test_analyze_table(self):
        self.storage.engine = MagicMock()
        self.storage._get_tablename = lambda: 'tablename'
        self.storage.analyze_table()

        self.storage.engine.connect.return_value.execute.assert_called_with('VACUUM ANALYZE tablename')

    def test_add_events(self):
        self.storage.session = MagicMock()
        metadata = fixtures.get_metadata_fixture()
        event = fixtures.get_event_fixture(metadata, "ADD")
        event["data"] = {
            "_source_id": "any source_id",
            "_tid": "abcd.1",
            "decimal": Decimal("1.0"),
            "datetime": datetime.datetime(2023, 1, 1, 13, 00),
            "date": datetime.date(2023, 1, 1),
            "int": 10
        }

        self.storage.add_events([event])

        query, params = self.storage.session.execute.call_args[0]

        assert str(query) == \
               "INSERT INTO events (eventid, timestamp, catalogue, entity, version, action, source, source_id, contents, application, tid) " \
                "VALUES (:eventid, :timestamp, :catalogue, :entity, :version, :action, :source, :source_id, :contents, :application, :tid)"

        assert params == [
            {
                "timestamp": self.storage.metadata.timestamp,
                "source": self.storage.metadata.source,
                "catalogue": self.storage.metadata.catalogue,
                "entity": self.storage.metadata.entity,
                "application": self.storage.metadata.application,
                "version": "0.9",
                "action": "ADD",
                "source_id": "any source_id",
                "contents": '{"_source_id": "any source_id", "_tid": "abcd.1", "decimal": 1.0,'
                            ' "datetime": "2023-01-01T13:00:00.000000", "date": "2023-01-01",'
                            ' "int": 10}',
                "tid": "abcd.1"
            }
        ]

        # no source id in data
        self.storage.session.reset_mock()
        event["data"] = {"_tid": "abcd.1"}
        self.storage.add_events([event])
        _, params = self.storage.session.execute.call_args[0]
        assert params[0]["source_id"] is None

    @patch("gobupload.storage.handler.SessionORM.scalars")
    @patch("gobupload.storage.handler.SessionORM.execute")
    def test_stream_session(self, mock_execute, mock_scalars):
        obj = StreamSession()
        default_opts = {"execution_options": {"yield_per": obj.YIELD_PER}}

        obj.stream_execute("query", extra=5)
        mock_execute.assert_called_with("query", **default_opts, extra=5)

        obj.stream_scalars("query", extra=5)
        mock_scalars.assert_called_with("query", **default_opts, extra=5)

        mock_execute.reset_mock()
        obj.stream_execute("query", extra=5, execution_options={"yield_per": 2000})
        mock_execute.assert_called_with("query", execution_options={"yield_per": 2000}, extra=5)
