"""Abstraction for the storage that is backing GOB, it is metadata aware, and requires a session in context.

Use it like this:

    message = ImportMessage(msg)
    metadata = message.metadata

    storage = GOBStorageHandler(metadata)
"""
from __future__ import annotations

import functools
import json
import warnings
import traceback
from contextlib import contextmanager
from typing import Union, Iterator, Iterable

from sqlalchemy import (
    create_engine, Table, update, exc as sa_exc, select, column, String, values, Column, text
)
from sqlalchemy.engine import Row
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import OperationalError, MultipleResultsFound
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, Session as SessionORM

from gobcore.enum import ImportMode
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.model import FIELD
from gobcore.model.sa.gob import get_column
from gobcore.model.sa.indexes import get_indexes
from gobcore.typesystem import get_gob_type
from gobcore.events.import_events import CONFIRM

from alembic.runtime import migration
import alembic.config
import alembic.script

from gobcore.typesystem.gob_types import JSON
from gobcore.typesystem.json import GobTypeJSONEncoder
from gobupload import gob_model
from gobupload.config import GOB_DB
from gobupload.storage import queries
from gobupload.storage.materialized_views import MaterializedViews

# not used but must be imported
# https://geoalchemy-2.readthedocs.io/en/latest/core_tutorial.html#reflecting-tables
from geoalchemy2 import Geometry  # noqa: F401


def with_session(func):
    """Decorator for methods that require the session in the context

    Use like this:

        @with_session
        def get_entity(self, id):

    A call to this method will raise an GOBException if session is not in context.
    """
    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        self = args[0]
        if self.session is None:
            raise GOBException("No current session")
        return func(*args, **kwargs)
    return wrapper_decorator


class StreamSession(SessionORM):
    """Extended session class with streaming functionality."""

    # default value for batchsize
    # only used if not specified otherwise
    # higher value leads to more memory allocation per cycle
    YIELD_PER = 2_000

    def _update_param(self, **kwargs) -> dict[str, dict]:
        exec_opts = kwargs.pop("execution_options", {})

        if "yield_per" not in exec_opts:
            exec_opts["yield_per"] = self.YIELD_PER

        return {"execution_options": exec_opts, **kwargs} if exec_opts else kwargs

    def stream_scalars(self, statement, **kwargs):
        """
        Execute a statement and return the results as scalars.
        Use a server-side cursor during statement execution to prevent high memory consumption.

        Batchsize can be adjusted by passing yield_per=<size> to execution_options
        """
        return super().scalars(statement, **self._update_param(**kwargs))

    def stream_execute(self, statement, **kwargs):
        """
        Execute a SQL expression construct.
        Use a server-side cursor during statement execution to prevent high memory consumption.

        Batchsize can be adjusted by passing yield_per=<size> to execution_options
        """
        return super().execute(statement, **self._update_param(**kwargs))


class GOBStorageHandler:
    """Metadata aware Storage handler."""
    engine = create_engine(
        URL.create(**GOB_DB),
        connect_args={'sslmode': 'require'},
        pool_pre_ping=True,
    )
    Session = sessionmaker(
        autocommit=True,
        autoflush=False,
        bind=engine,
        class_=StreamSession
    )
    base = automap_base(bind=engine)

    @classmethod
    def _set_base(cls, update=False, reflection_options: dict = None):
        reflection_options = reflection_options or {}
        only = reflection_options.get("only")
        reflections_exist = only and all(hasattr(cls.base.classes, table) for table in only)

        # no reflection necessary
        if not (update or reflection_options) or reflections_exist:
            return

        # reflection required, clear first
        cls.base.metadata.clear()

        print(f"Reflecting {only}" if only else "Reflecting database")

        with warnings.catch_warnings():
            # Ignore warnings for unsupported reflection for expression-based indexes
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)

            # Reflect database in metadata, prepare generates mapped classes
            cls.base.metadata.reflect(**reflection_options)
            cls.base.prepare()

    EVENTS_TABLE = "events"

    user_name = f"({GOB_DB['username']}@{GOB_DB['host']}:{GOB_DB['port']})"

    WARNING = 'warning'
    ERROR = 'error'

    config_checks = [
        # (setting, check, message, error/warning)
        ('default_statistics_target', lambda x: int(x) >= 1000, 'should be greater than or equal to 1000', WARNING),
        ('enable_partition_pruning', lambda x: x == 'on', 'should be set to on', ERROR),
        ('constraint_exclusion', lambda x: x == 'on', 'should be set to on', WARNING),
    ]

    def __init__(self, gob_metadata=None, **reflection_options):
        """
        Initialize StorageHandler with gob metadata.
        This will create abstractions to entities and events, and initialize storage if necessary.
        When gob_metadata is passed, only the necessary tables will be reflected if not already available
        in the metadata.

        :param gob_metadata: A metadata object containing attributes: source, catalogue and entity
            This parameter always triggers a reflect operation
        :param reflection_options: Reflect database with these options,
            eg {"only": ["events"}} to only reflect the events table
        """
        self.metadata = gob_metadata
        self.session: StreamSession | None = None

        if gob_metadata:
            self._fields = self.get_collection_model()["all_fields"]
            self._field_types = {field: get_gob_type(self._fields[field]["type"]) for field in self._fields}

            reflection_options["only"] = [self.EVENTS_TABLE, self.tablename] + reflection_options.get("only", [])

        GOBStorageHandler._set_base(reflection_options=reflection_options)

    def init_storage(
            self,
            force_migrate=False,
            recreate_materialized_views: Union[bool, list] = False,
    ):
        """Check if the necessary tables (for events, and for the entities in gobmodel) are present
        If not, they are required

        :param force_migrate: Don't wait for any migrations to finish before continuing
        :param recreate_materialized_views: List of mv's to recreate, True for all, False for none
        """
        MIGRATION_LOCK = 19935910  # Just some random number

        if not force_migrate:
            # Don't force
            # Nicely wait for any migrations to finish before continuing
            self.engine.execute(f"SELECT pg_advisory_lock({MIGRATION_LOCK})")

        try:
            # Check if storage is up-to-date
            alembic_cfg = alembic.config.Config('alembic.ini')
            script = alembic.script.ScriptDirectory.from_config(alembic_cfg)
            with self.engine.begin() as conn:
                context = migration.MigrationContext.configure(conn)
                up_to_date = context.get_current_revision() == script.get_current_head()

            if not up_to_date:
                print('Migrating storage')
                alembic.config.main(argv=['--raiseerr', 'upgrade', 'head'])

            # refresh reflected base
            self._set_base(update=True)

            # Create necessary indexes
            self._init_indexes()

            # Initialise materialized views for relations
            self._init_relation_materialized_views(recreate_materialized_views)

        except Exception as err:
            print(f'Storage migration failed: {str(err)}')
            raise err
        else:  # No exception
            print('Storage is up-to-date')
        finally:
            # Always unlock
            self.engine.execute(f"SELECT pg_advisory_unlock({MIGRATION_LOCK})")

        self._check_configuration()

    def _get_config_value(self, setting: str):
        return next(self.engine.execute(f"SHOW {setting}"))[0]

    def _check_configuration(self):
        for setting, check, message, type in self.config_checks:
            value = self._get_config_value(setting)
            if not check(value):
                msg = f"Checking Postgres config for {setting}. Value is {value}, but {message}"
                if type == self.ERROR:
                    raise GOBException(msg)
                else:
                    print(f"WARNING: {msg}")

    def _init_relation_materialized_views(self, recreate=False):
        mv = MaterializedViews()
        mv.initialise(self, recreate)

    def _get_index_type(self, type: str) -> str:
        if type == "geo":
            return "GIST"
        elif type == "json":
            return "GIN"
        else:
            return "BTREE"

    def _indexes_to_drop_query(self, tablenames: list, keep_indexes: list):
        keep = ','.join([f"'{index}'" for index in keep_indexes])
        relations = ','.join([f"'{tablename}'" for tablename in tablenames])

        return f"""
SELECT
    s.indexrelname
FROM pg_catalog.pg_stat_user_indexes s
JOIN pg_catalog.pg_index i ON s.indexrelid = i.indexrelid
WHERE
    s.relname in ({relations})
    AND s.indexrelname not in ({keep})
    AND 0 <> ALL (i.indkey)    -- no index column is an expression
    AND NOT i.indisunique  -- no unique indexes
    AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint c WHERE c.conindid = s.indexrelid)
"""

    def _drop_indexes(self, indexes):
        """Drops indexes on managed tables that aren't defined in this script.

        :return:
        """
        query = self._indexes_to_drop_query([index['table_name'] for index in indexes.values()], list(indexes.keys()))

        try:
            indexes_to_drop = self.engine.execute(query)
        except OperationalError as e:
            print(f"ERROR: Could not get indexes to drop: {e}")
            return

        for index in indexes_to_drop:
            try:
                statement = f'DROP INDEX IF EXISTS "{index[0]}"'
                self.execute(statement)
            except OperationalError as e:
                print(f"ERROR: Could not drop index {index[0]}: {e}")

    def _get_existing_indexes(self) -> list:
        query = "SELECT indexname FROM pg_indexes"

        try:
            return [row[0] for row in self.engine.execute(query)]
        except OperationalError as e:
            print(f"WARNING: Could not fetch list of existing indexes: {e}")
            return []

    def _init_indexes(self):
        """Create indexes

        :return:
        """
        indexes = get_indexes(gob_model)
        self._drop_indexes(indexes)
        existing_indexes = self._get_existing_indexes()

        for name, definition in indexes.items():
            if name in existing_indexes:
                # Don't run CREATE INDEX IF NOT EXISTS query to prevent unnecessary locks
                continue

            columns = ','.join(definition['columns'])
            index_type = self._get_index_type(definition.get('type'))
            statement = f"CREATE INDEX IF NOT EXISTS \"{name}\" " \
                f"ON {definition['table_name']} USING {index_type}({columns})"

            try:
                self.execute(statement)
            except OperationalError as e:
                print(f"ERROR: Index {name} failed: {e}")

    @with_session
    def create_temporary_table(self):
        """
        Create a new temporary table based on the current table for a collection.
        Add table to current metadata stored in `base`.
        The table will be dropped when the connection is released.
        """
        if self.tablename_temp in self.base.metadata.tables:
            raise ValueError(f"Temporary table exists in metadata: {self.tablename_temp}")

        columns: list[Column] = [
            get_column(FIELD.TID, self._fields[FIELD.TID]),
            get_column(FIELD.SOURCE, self._fields[FIELD.SOURCE]),
            get_column(FIELD.HASH, self._fields[FIELD.HASH]),
            JSON.get_column_definition("_original_value")
        ]

        table = Table(
            self.tablename_temp,
            self.base.metadata,
            *columns,
            implicit_returning=False,  # no returning on insert
            prefixes=["TEMPORARY"]     # CREATE TEMPORARY TABLE <table>
        )
        table.create(bind=self.session.bind)

    @with_session
    def write_temporary_entities(self, entities):
        """
        Writes the temporary entities to the temporary table

        If no arguments are given the write will always take place
        If the write_per argument is specified writes will take place in chunks
        :return:
        """
        table = self.base.metadata.tables[self.tablename_temp]
        tid_type = self._field_types[FIELD.TID]
        hash_type = self._field_types[FIELD.HASH]
        source_value = self._field_types[FIELD.SOURCE].from_value(self.metadata.source).to_db

        rows = [
            {
                FIELD.TID: tid_type.from_value(entity[FIELD.TID]).to_db,
                FIELD.SOURCE: source_value,
                FIELD.HASH: hash_type.from_value(entity[FIELD.HASH]).to_db,
                "_original_value": entity
            }
            for entity in entities
        ]
        self.session.execute(table.insert(), rows)

    @with_session
    def compare_temporary_data(self, mode: ImportMode = ImportMode.FULL) -> Iterator[Row]:
        """ Compare the data in the temporay table to the current state

        The created query compares each model field and returns the tid, last_event
        _hash and if the record should be a ADD, DELETE or MODIFY. CONFIRM records are not
        included in the result, but can be derived from the message

        :return: a iterator of dicts with tid, hash, last_event and type
        """
        query = queries.get_comparison_query(
            source=self.metadata.source,
            current=self.tablename,
            temporary=self.tablename_temp,
            fields=[FIELD.TID],
            mode=mode
        )
        yield from self.session.stream_execute(query, execution_options={"yield_per": 10_000})

    @with_session
    def analyze_temporary_table(self):
        """Runs VACUUM ANALYZE on temporary table."""
        conn = self.session.bind

        # Temporary switch to database AUTOCOMMIT, reset after VACUUM
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text(f"VACUUM ANALYZE {self.tablename_temp}"))
        conn.execution_options(isolation_level=conn.default_isolation_level)

    @property
    def DbEvent(self):
        return getattr(self.base.classes, self.EVENTS_TABLE)

    @property
    def DbEntity(self):
        return getattr(self.base.classes, self.tablename)

    @property
    def tablename(self) -> str | None:
        if self.metadata:
            return gob_model.get_table_name(self.metadata.catalogue, self.metadata.entity)

    @property
    def tablename_temp(self) -> str | None:
        if self.metadata:
            return self.tablename + "_tmp"

    @contextmanager
    def get_session(self, invalidate: bool = False) -> StreamSession:
        """
        Exposes an underlying database session as a managed, not transactional, context.
        Isolation level for this context is 'autocommmit' through sessionmaker.

        :param invalidate: Invalidate and release connection on exiting context.
            Useful in case you want to discard the database session, i.e. to drop temp table
        """
        connection = self.engine.connect()
        self.session = self.Session(bind=connection)

        try:
            yield self.session
        except Exception as err:
            print(traceback.format_exc(limit=-5))
            logger.error(repr(err))
            self.session.rollback()
        else:
            self.session.flush()
        finally:
            if invalidate:
                # release connection to database -> dropping temp tables
                connection.invalidate()

                # make sure temp table is removed from the (base) metadata class var
                if (tmp_table := self.base.metadata.tables.get(self.tablename_temp)) is not None:
                    self.base.metadata.remove(tmp_table)

            self.session.close()
            self.session = None

    @with_session
    def get_entity_max_eventid(self) -> int:
        """Get the highest last_event property of entity

        :return: The highest last_event
        """
        last_event = getattr(self.DbEntity, "_last_event")
        query = (
            select(last_event)
            .where(self.DbEntity._source == self.metadata.source)
            .order_by(last_event.desc())
            .limit(1)
        )
        return self.session.execute(query).scalar() or 0

    @with_session
    def get_last_eventid(self) -> int:
        """Get the highest last_event property of entity

        :return: The highest last_event
        """
        events = self.DbEvent
        query = (
            select(events.eventid)
            .where(events.source == self.metadata.source)
            .where(events.catalogue == self.metadata.catalogue)
            .where(events.entity == self.metadata.entity)
            .order_by(events.eventid.desc())
            .limit(1)
        )
        return self.session.execute(query).scalar() or 0

    def get_events_starting_after(self, eventid: int, limit: int = 10_000) -> Iterator[list[Row]]:
        """
        Return chunks of events with eventid starting at `eventid`.
        Example with size=2: ([event1, event2], [event3, event4], [event5])
        This process can take a long time for big collections, keep this in a seperate session.
        Use pagination by eventid instead of streaming results to prevent locking the events table.

        :param eventid: minimal eventid (0 for all)
        :param limit: limit returned result to this size
        :return: Iterator containing lists of events
        """
        events = self.DbEvent
        query = (
            select(
                events.eventid,
                events.timestamp,
                events.catalogue,
                events.entity,
                events.version,
                events.action,
                events.source,
                events.contents,
                events.application,
                events.tid
            )
            .where(events.source == self.metadata.source)
            .where(events.catalogue == self.metadata.catalogue)
            .where(events.entity == self.metadata.entity)
            .order_by(events.eventid.asc())
            .limit(limit)
        )

        def _get_chunk(after_event: int) -> list[Row]:
            # close connection after every query, release transaction
            with self.engine.connect() as conn:
                return conn.execute(query.where(events.eventid > after_event)).all()

        start_after = eventid

        while chunk := _get_chunk(start_after):
            yield chunk
            start_after = getattr(chunk[-1], "eventid")

    @with_session
    def has_any_event(self, filter_: dict) -> bool:
        """True if any event matches the filter condition

        :return: true is any event has been found given the filter
        """
        query = select(self.DbEvent.eventid).limit(1)

        for key, val in filter_.items():
            query = query.where(getattr(self.DbEvent, key) == val)

        return self.session.execute(query).first() is not None

    @with_session
    def has_any_entity(self, key=None, value=None):
        """Check if any entity exist with the given key-value combination
        When no key-value combination is supplied check for any entity

        :param key: key value, e.g. "_source"
        :param value: value to loop for, e.g. "DIVA"
        :return: True if any entity exists, else False
        """
        query = select(self.DbEntity).limit(1)

        if key and value:
            query = query.where(getattr(self.DbEntity, key) == value)
        return self.session.execute(query).first() is not None

    def get_collection_model(self):
        if self.metadata.catalogue in gob_model:
            return gob_model[self.metadata.catalogue]['collections'].get(self.metadata.entity)
        return None

    @with_session
    def get_current_ids(self, exclude_deleted=True) -> Iterator[str]:
        """Overview of entities that are current

        Current id's are evaluated within an application

        :return: a list of ids for the entity that are currently not deleted.
        """
        query = select(self.DbEntity._tid)
        if exclude_deleted:
            query = query.where(self.DbEntity._date_deleted.is_(None))
        return self.session.stream_scalars(query)

    @with_session
    def get_last_events(self) -> dict[str, int]:
        """Overview of all last applied events for the current collection

        :return: a dict of ids with last_event for the collection
        """
        query = select(self.DbEntity._tid, self.DbEntity._last_event)
        return {row[0]: row[1] for row in self.session.stream_execute(query)}

    @with_session
    def get_column_values_for_key_value(self, column, key, value):
        """Gets the distinct values for column within the given source for the given key-value

        Example: get all values for column "identification" with "code" == "A" coming from source "AMSBI"

        :param column: Name of the column for which to return the unique values
        :param key: Name of the column to filter on its value
        :param value: The value to filter the column on
        :return: A list of all unique values for the given combination within the Storage handler source
        """
        filter = {
            "_source": self.metadata.source,
            key: value
        }
        attr = getattr(self.DbEntity, column)
        return self.session.query(attr) \
            .filter_by(**filter) \
            .filter(attr.isnot(None)) \
            .distinct(attr) \
            .all()

    @with_session
    def get_last_column_value(self, template, column):
        """Get the "last" value for column with column values that match the template

        Example: Get the last value for "identification" with values that match "036%"

        The last value is defined by the order_by clause.

        :param template: A template string, eg "036%"
        :param column: The column to filter on, eg "identification"
        :return:
        """
        filter = {
            "_source": self.metadata.source
        }
        attr = getattr(self.DbEntity, column)
        return self.session.query(attr) \
            .filter_by(**filter) \
            .filter(attr.like(template)) \
            .order_by(attr.desc()) \
            .limit(1) \
            .value(attr)

    @with_session
    def get_current_entity(self, entity, with_deleted=False):
        """Gets current stored version of entity for the given entity.

        If it doesn't exist, returns None

        An entity to retrieve is evaluated within a source
        on the basis of its functional id (_id)

        :param entity: the new version of the entity
        :raises GOBException:
        :return: the stored version of the entity, or None if it doesn't exist
        """
        filter = {"_tid": entity['_tid']}

        entity_query = self.session.query(self.DbEntity).filter_by(**filter)
        if not with_deleted:
            entity_query = entity_query.filter_by(_date_deleted=None)

        try:
            return entity_query.one_or_none()
        except MultipleResultsFound:
            filter_str = ','.join([f"{k}={v}" for k, v in filter.items()])
            raise GOBException(f"Found multiple rows with filter: {filter_str}")

    @with_session
    def get_entities(self, tids: Iterable[str], with_deleted=False) -> Iterator[Row]:
        """
        Get entities with tid contained in the given list of tid's

        :param tids: ids of the entities to get
        :param with_deleted: boolean denoting if entities that are deleted should be considered (default: False)
        :return:
        """
        values_tid = \
            values(column("_tid", String), name="tids")\
            .data([(tid, ) for tid in tids])

        query = select(self.DbEntity).join(values_tid, self.DbEntity._tid == values_tid.c._tid)

        if not with_deleted:
            query = query.where(self.DbEntity._date_deleted.is_(None))

        return self.session.stream_scalars(query)

    @with_session
    def add_add_events(self, events):
        table = self.DbEntity.__table__
        rows = [event.get_attribute_dict() | {"_last_event": event.id} for event in events]
        self.session.execute(table.insert(), rows)

    @with_session
    def add_events(self, events):
        """
        Add the given events to the events table

        :param events: the list of events to insert
        :return: None
        """
        table = self.DbEvent.__table__
        timestamp = self.metadata.timestamp
        source = self.metadata.source
        catalogue = self.metadata.catalogue
        entity = self.metadata.entity
        application = self.metadata.application

        rows = [
            {
                "timestamp": timestamp,
                "source": source,
                "catalogue": catalogue,
                "entity": entity,
                "application": application,
                "version": event['version'],
                "action": event['event'],
                "source_id": event["data"].get("_source_id"),
                "contents": json.dumps(event["data"], cls=GobTypeJSONEncoder),
                "tid": event["data"]["_tid"]
            }
            for event in events
        ]
        # disable implicit returning
        # https://docs.sqlalchemy.org/en/14/core/dml.html#sqlalchemy.sql.expression.Insert.inline
        self.session.execute(table.insert().inline(), rows)

    def apply_confirms(self, confirms, timestamp):
        """
        Apply a (BULK)CONFIRM event

        :param confirms: list of confirm data
        :param timestamp: Time to set as last_confirmed
        :return:
        """
        values_tid = \
            values(column("_tid", String), name="tids") \
            .data([(record['_tid'],) for record in confirms])

        stmt = (
            update(self.DbEntity)
            .where(self.DbEntity._tid == values_tid.c._tid)
            .values({CONFIRM.timestamp_field: timestamp})
        )
        self.execute(stmt)

    def execute(self, statement):
        result = self.engine.execute(statement)
        result.close()

    def get_query_value(self, query):
        """Execute a query and return the result value

        The supplied query needs to resolve to a scalar value

        :param query: Query string
        :return: scalar value result
        """
        result = self.engine.execute(query)
        value = result.scalar()
        result.close()
        return value

    def get_source_catalogue_entity_combinations(self, **kwargs) -> list[Row]:
        """Return all unique source / catalogue / entity combinations."""
        query = select(
            self.DbEvent.source,
            self.DbEvent.catalogue,
            self.DbEvent.entity
        ).distinct()

        for key, val in kwargs.items():
            if hasattr(self.DbEvent, key) and val:
                query = query.where(getattr(self.DbEvent, key) == val)

        with self.engine.connect() as conn:
            return conn.execute(query).all()

    def analyze_table(self):
        """Runs VACUUM ANALYZE on table

        :return:
        """
        # Create separate connection and start with COMMIT to be outside of transaction context, otherwise VACUUM won't
        # work.
        connection = self.engine.connect()
        connection.execute("COMMIT")
        connection.execute(f"VACUUM ANALYZE {self.tablename}")
        connection.close()
