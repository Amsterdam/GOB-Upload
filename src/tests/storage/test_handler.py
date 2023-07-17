import datetime
import unittest
from decimal import Decimal
from unittest.mock import call, MagicMock, patch, ANY, PropertyMock

from sqlalchemy import Integer, DateTime, String, JSON, text, Engine, select
from sqlalchemy.engine import Connection
from sqlalchemy.orm import declarative_base

from gobcore.events.import_message import ImportMessage
from gobcore.exceptions import GOBException

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


class MockMeetbouten(Base):
    __tablename__ = "meetbouten_meetbouten"

    eventid = sa.Column(Integer, primary_key=True)
    timestamp = sa.Column(DateTime)
    catalogue = sa.Column(String)
    entity = sa.Column(String)
    _tid = sa.Column(String)
    _date_confirmed = sa.Column(DateTime)


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

        with patch("gobupload.storage.handler.random_string", MagicMock(return_value="abcdefgh")):
            self.storage = GOBStorageHandler(metadata)

        GOBStorageHandler.engine = MagicMock()
        GOBStorageHandler.engine.__enter__ = lambda self: None
        GOBStorageHandler.engine.__exit__ = lambda *args: None
        GOBStorageHandler.engine.begin = lambda: GOBStorageHandler.engine

        GOBStorageHandler.base.classes.events = MockEvents
        GOBStorageHandler.base.classes.meetbouten_meetbouten = MockMeetbouten

    @patch("gobupload.storage.handler.automap_base")
    def test_base(self, mock_base):
        # no reflection
        GOBStorageHandler.base = mock_base
        GOBStorageHandler._set_base()
        mock_base.prepare.assert_not_called()

        # automap is called, update = True
        mock_base.reset_mock()
        GOBStorageHandler._set_base(update=True)
        mock_base.prepare.assert_called_once()

        # no reflection, update = False
        mock_base.reset_mock()
        GOBStorageHandler._set_base(update=False)
        mock_base.prepare.assert_not_called()

        # reflection options
        mock_base.reset_mock()
        GOBStorageHandler._set_base(reflection_options={"opt1": "val1"})
        mock_base.metadata.reflect.assert_called_with(GOBStorageHandler.engine, opt1="val1")

        # reflection options, only kwarg, table exists
        mock_base.reset_mock()
        GOBStorageHandler.base.classes.table1 = "table1_properties"
        GOBStorageHandler._set_base(reflection_options={"only": ["table1"]})
        mock_base.metadata.reflect.assert_not_called()

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

    @patch("gobupload.storage.handler.random_string", MagicMock(return_value="abcdefgh"))
    @patch("gobupload.storage.handler.GOBStorageHandler.get_collection_model", MagicMock())
    def test_tablename_temp(self):
        """
        Test whether repeated calls to tablename_temp yield the same result.
        Different metadata yields different tablenames.
        """
        class Meta:
            catalogue = "meetbouten"
            entity = "meetbouten"
            source = "any source1"

        storage = GOBStorageHandler(Meta())
        assert "tmp_meetbouten_meetbouten_abcdefgh" == storage.tablename_temp

        class OtherMeta:
            catalogue = "meetbouten"
            entity = "very long enity" * 10
            source = "any source2"

        other = GOBStorageHandler(OtherMeta())
        assert "tmp_meetbouten_very long enityvery long enityvery long_abcdefgh" == other.tablename_temp
        assert len(other.tablename_temp) == 63

    @patch("gobupload.storage.handler.text")
    @patch("builtins.print")
    def test_check_configuration(self, mock_print, mock_text):
        mock_conn = self.storage.engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.scalar_one.return_value = 'the value'

        self.storage.config_checks = [
            ('the setting', lambda x: True, 'the message', self.storage.WARNING)
        ]

        self.storage._check_configuration()
        mock_print.assert_not_called()
        mock_text.assert_called_with("SHOW the setting")

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

    @patch("gobupload.storage.handler.text")
    def test_drop_indexes(self, mock_text):
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

        self.storage._indexes_to_drop_query = MagicMock()

        mock_conn = self.storage.engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.scalars.return_value = ["index_c", "index_d"]

        self.storage._drop_indexes(indexes)

        mock_text.assert_has_calls([
            call('DROP INDEX IF EXISTS "index_c"'),
            call('DROP INDEX IF EXISTS "index_d"'),
        ])

        self.storage._indexes_to_drop_query.assert_called_with(
            ['sometable_1', 'sometable_2'],
            ['index_a', 'index_b'],
        )
        mock_text.assert_any_call(self.storage._indexes_to_drop_query.return_value)

    def test_get_existing_indexes(self):
        mock_conn = self.storage.engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.scalars.return_value.all.return_value = ['indexA', 'indexB']

        assert ['indexA', 'indexB'] == self.storage._get_existing_indexes()

    @patch("gobupload.storage.handler.text")
    @patch('gobupload.storage.handler.get_indexes')
    def test_init_indexes(self, mock_get_indexes, mock_text):
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

        mock_conn = self.storage.engine.connect.return_value.__enter__.return_value
        mock_text.assert_has_calls([
            call("CREATE INDEX IF NOT EXISTS \"indexname\" ON sometable USING BTREE(cola,colb)"),
            call("CREATE INDEX IF NOT EXISTS \"index2name\" ON someothertable USING BTREE(cola)"),
            call("CREATE INDEX IF NOT EXISTS \"geo_index\" ON table_with_geo USING GIST(geocol) WHERE ST_IsValid(geocol)"),
            call("CREATE INDEX IF NOT EXISTS \"json_index\" ON table_with_json USING GIN(somejsoncol)"),
        ])
        assert mock_conn.execute.call_count == 4
        self.storage._drop_indexes.assert_called_once()

    @patch("gobupload.storage.handler.Table")
    def test_create_temporary_table(self, mock_table):
        mock_session = MagicMock(spec=StreamSession)
        mock_session.bind = MagicMock(spec=Connection)
        self.storage.session = mock_session

        self.storage.create_temporary_table()

        mock_table.assert_called_with(
            "tmp_meetbouten_meetbouten_abcdefgh",
            self.storage.base.metadata,
            *[ANY] * 4,  # columns
            implicit_returning=False,
            prefixes=["TEMPORARY"]
        )
        mock_table.return_value.create.assert_called_with(bind=mock_session.bind)

        with patch.object(self.storage.base.metadata, "tables", {"tmp_meetbouten_meetbouten_abcdefgh": 1}):
            with self.assertRaises(ValueError):
                self.storage.create_temporary_table()

    def test_write_temporary_entities(self):
        mock_session = MagicMock(spec=StreamSession)
        mock_session.stream_execute.return_value = [{"any": "value"}]
        self.storage.session = mock_session

        entities = [{"_tid": "1", "_hash": "any", "_id": "any id"}]
        self.storage.write_temporary_entities(entities)

        expected = [{
            "_tid": "1",
            "_source": "any source",
            "_hash": "any",
            "_original_value": {"_tid": "1", "_hash": "any", "_id": "any id"}
        }]
        mock_session.execute.assert_called_with(
            self.storage.base.metadata.tables.__getitem__.return_value.insert.return_value,
            expected
        )

    def test_compare_temporary_data(self):
        mock_session = MagicMock(spec=StreamSession)
        row = type("Row", (object, ), {"any": "value"})
        mock_session.stream_execute.return_value.partitions.return_value = yield [row]
        self.storage.session = mock_session

        current = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}'
        temporary = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}_tmp'
        query = queries.get_comparison_query("any source", current, temporary, ["_tid"])

        diff = self.storage.compare_temporary_data()

        for result in diff:
            assert result == [row]
        mock_session.stream_execute.assert_called_with(query)
        mock_session.stream_execute.return_value.partitions.assert_called_once()

    @patch("gobupload.storage.handler.text")
    def test_analyze_temporary_table(self, mock_text):
        mock_session = MagicMock(spec=StreamSession)
        mock_session.bind = MagicMock(spec=Connection)
        self.storage.session = mock_session

        self.storage.analyze_temporary_table()

        mock_text.assert_called_with("ANALYZE tmp_meetbouten_meetbouten_abcdefgh")
        mock_session.bind.execute.assert_called_with(
            mock_text.return_value, execution_options={"isolation_level": "AUTOCOMMIT"}
        )

    @patch("gobupload.storage.handler.text")
    def test_get_query_value(self, mock_text):
        result = self.storage.get_query_value('SELECT * FROM test')

        mock_conn = self.storage.engine.connect.return_value.__enter__.return_value
        assert mock_conn.execute.return_value.scalar.return_value == result
        mock_text.assert_called_with('SELECT * FROM test')

    def test_combinations_plain(self):
        mock_conn = MagicMock(spec=Connection)
        mock_conn.__enter__.return_value.execute.return_value.all.return_value = ["row1"]
        self.storage.engine.connect.return_value = mock_conn

        result = self.storage.get_source_catalogue_entity_combinations()
        assert result == ["row1"]

        query = mock_conn.__enter__.return_value.execute.call_args[0][0]
        query = str(query.compile(compile_kwargs={"literal_binds": True}))
        expected = "SELECT DISTINCT events.source, events.catalogue, events.entity \nFROM events"
        assert query == expected

    def test_combinations_with_args(self):
        mock_conn = MagicMock(spec=Connection)
        mock_conn.__enter__.return_value.execute.return_value.all.return_value = ["row1"]
        self.storage.engine.connect.return_value = mock_conn

        result = self.storage.get_source_catalogue_entity_combinations(source="val")
        assert result == ["row1"]

        query = mock_conn.__enter__.return_value.execute.call_args[0][0]
        query = str(query.compile(compile_kwargs={"literal_binds": True}))
        expected = "\n".join([
            "SELECT DISTINCT events.source, events.catalogue, events.entity ",
            "FROM events ",
            "WHERE events.source = 'val'"
        ])
        assert query == expected

    @patch("gobupload.storage.handler.text")
    def test_analyze_table(self, mock_text):
        self.storage.engine = MagicMock(spec=Engine)
        self.storage.analyze_table()

        mock_text.assert_called_with("VACUUM ANALYZE meetbouten_meetbouten")

        self.storage.engine.connect.return_value.execution_options.assert_called_with(isolation_level="AUTOCOMMIT")
        self.storage.engine.connect.return_value.execution_options.return_value.__enter__.return_value \
            .execute.assert_called_with(mock_text.return_value)

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

    @patch("gobupload.storage.handler.text")
    @patch("gobupload.storage.handler.SessionORM.scalars")
    @patch("gobupload.storage.handler.SessionORM.execute")
    def test_stream_session(self, mock_execute, mock_scalars, mock_text):
        obj = StreamSession()
        default_opts = {"execution_options": {"stream_results": True, "yield_per": obj.YIELD_PER}}

        obj.stream_execute("query", extra=5)
        mock_text.assert_called_with("query")
        mock_execute.assert_called_with(mock_text.return_value, **default_opts, extra=5)

        obj.stream_scalars("query2", extra=5)
        mock_text.assert_called_with("query2")
        mock_scalars.assert_called_with(mock_text.return_value, **default_opts, extra=5)

        obj.stream_execute("query3", extra=5, execution_options={"yield_per": 2000})
        mock_text.assert_called_with("query3")
        mock_execute.assert_called_with(
            mock_text.return_value,
            execution_options={"yield_per": 2000, "stream_results": True},
            extra=5
        )

        stmt = select(self.storage.DbEvent),
        obj.stream_execute(stmt, extra=5)
        mock_execute.assert_called_with(stmt, **default_opts, extra=5)

        obj.stream_scalars(stmt, extra=5)
        mock_execute.assert_called_with(stmt, **default_opts, extra=5)

        obj.execute(stmt, extra=5)
        mock_execute.assert_called_with(stmt, extra=5)

    def test_apply_confirms(self):
        mock_session = MagicMock(spec=StreamSession)
        self.storage.session = mock_session

        confirms = [{"_tid": "confirm1"}, {"_tid": "confirm2"}]
        timestamp = datetime.datetime(2023, 6, 6)

        self.storage.apply_confirms(confirms, timestamp)

        query = mock_session.execute.call_args[0][0]
        query = str(query.compile(compile_kwargs={"literal_binds": True}))

        expected = (
            "UPDATE meetbouten_meetbouten "
            "SET _date_confirmed='2023-06-06 00:00:00' "
            "FROM (VALUES ('confirm1'), ('confirm2')) AS tids (_tid) "
            "WHERE meetbouten_meetbouten._tid = tids._tid"
        )
        assert query == expected

    @patch("gobupload.storage.handler.StreamSession", spec=StreamSession)
    def test_get_events_starting_after(self, mock_session):
        mock_row = MockEvents(eventid=14)
        mock_sess = mock_session.return_value.__enter__.return_value
        mock_sess.scalars.return_value.all.return_value = [mock_row]

        result = self.storage.get_events_starting_after(12)
        assert result[0].eventid == 14

        query = mock_sess.scalars.call_args_list[0][0][0]
        query = str(query.compile(compile_kwargs={"literal_binds": True}))

        expected = "\n".join([
            "SELECT events.eventid, events.timestamp, events.catalogue, "
            "events.entity, events.version, events.action, events.source, "
            "events.source_id, events.contents, events.application, events.tid ",
            "FROM events ",
            "WHERE events.source = 'any source' "
            "AND events.catalogue = 'meetbouten' "
            "AND events.entity = 'meetbouten' "
            "AND events.eventid > 12 "
            "ORDER BY events.eventid ASC",
            " LIMIT 10000"
        ])
        assert query == expected
