"""Abstraction for the storage that is backing GOB, it is metadata aware, and requires a session in context.

Use it like this:

    message = ImportMessage(msg)
    metadata = message.metadata

    storage = GOBStorageHandler(metadata)
"""
from __future__ import annotations

import datetime
import functools
import json
import warnings

from contextlib import contextmanager
from typing import Union, Iterator, Iterable, Any, Sequence

from sqlalchemy import (
    create_engine, Table, update, exc as sa_exc, select, column, String, values, Column, text,
    Executable, Result, ScalarResult
)
from sqlalchemy.engine import Row
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import MultipleResultsFound
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
from gobupload.utils import random_string

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
        exec_opts["stream_results"] = True  # always enable streaming, for ie .partitions()

        if "yield_per" not in exec_opts:
            exec_opts["yield_per"] = self.YIELD_PER

        return {"execution_options": exec_opts, **kwargs} if exec_opts else kwargs

    def stream_scalars(self, statement: str | Executable, **kwargs) -> ScalarResult:
        """
        Execute a statement and return the results as scalars.
        Use a server-side cursor during statement execution to prevent high memory consumption.

        Batchsize can be adjusted by passing yield_per=<size> to execution_options
        """
        stmt = text(statement) if isinstance(statement, str) else statement
        return super().scalars(stmt, **self._update_param(**kwargs))

    def stream_execute(self, statement: str | Executable, **kwargs) -> Result:
        """
        Execute a SQL expression construct.
        Use a server-side cursor during statement execution to prevent high memory consumption.

        Batchsize can be adjusted by passing yield_per=<size> to execution_options
        """
        stmt = text(statement) if isinstance(statement, str) else statement
        return super().execute(stmt, **self._update_param(**kwargs))

    def execute(self, statement: str | Executable, *args, **kwargs) -> Result:
        stmt = text(statement) if isinstance(statement, str) else statement
        return super().execute(stmt, *args, **kwargs)


class GOBStorageHandler:
    """Metadata aware Storage handler."""

    engine = create_engine(
        URL.create(**GOB_DB),
        connect_args={'sslmode': 'require'},
        # https://docs.sqlalchemy.org/en/20/core/pooling.html#disconnect-handling-pessimistic
        pool_pre_ping=True,
        # https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#psycopg2-fast-execution-helpers
        executemany_mode="values_plus_batch",
        executemany_batch_page_size=10_000,
        # https://docs.sqlalchemy.org/en/20/core/connections.html#insert-many-values-behavior-for-insert-statements
        use_insertmanyvalues=False,
    )

    Session = sessionmaker(engine, class_=StreamSession, autoflush=False)
    base = automap_base()

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
            cls.base.metadata.reflect(cls.engine, **reflection_options)
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
            self.tablename_temp = self._generate_temp_tablename(gob_metadata)

            reflection_options["only"] = [self.EVENTS_TABLE, self.tablename] + reflection_options.get("only", [])

        GOBStorageHandler._set_base(reflection_options=reflection_options)

    def execute(self, statement: str, **kwargs):
        """Execute statement and commit to database, rollback if error occurs."""
        with self.engine.connect() as connection:
            connection.execute(text(statement), **kwargs)
            connection.commit()

    def init_storage(
        self,
        force_migrate=False,
        recreate_materialized_views: Union[bool, list] = False
    ):
        """Check if the necessary tables (for events, and for the entities in gobmodel) are present
        If not, they are required

        :param force_migrate: Don't wait for any migrations to finish before continuing
        :param recreate_materialized_views: List of mv's to recreate, True for all, False for none
        """
        MIGRATION_LOCK = 19935910

        if not force_migrate:
            self.execute(f"SELECT pg_advisory_lock({MIGRATION_LOCK})")

        try:
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
        else:
            print('Storage is up-to-date')
        finally:
            self.execute(f"SELECT pg_advisory_unlock({MIGRATION_LOCK})")

        self._check_configuration()

    def _check_configuration(self):
        with self.engine.connect() as connection:
            for setting, check, message, type_ in self.config_checks:
                value = connection.execute(text(f"SHOW {setting}")).scalar_one()
                if not check(value):
                    msg = f"Checking Postgres config for {setting}. Value is {value}, but {message}"
                    if type_ == self.ERROR:
                        raise GOBException(msg)
                    else:
                        print(f"WARNING: {msg}")

    def _init_relation_materialized_views(self, recreate=False):
        mv = MaterializedViews()
        mv.initialise(self, recreate)

    def _get_index_type(self, type_: str) -> str:
        return {"geo": "GIST", "json": "GIN"}.get(type_, "BTREE")

    def _indexes_to_drop_query(self, tablenames: list, keep_indexes: list) -> str:
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

    def _drop_indexes(self, indexes: dict[str, dict]):
        """Drops indexes on managed tables that aren't defined in this script. """
        query = self._indexes_to_drop_query([index['table_name'] for index in indexes.values()], list(indexes.keys()))

        with self.engine.connect() as connection:
            indexes_to_drop = connection.execute(text(query)).scalars()

            for index in indexes_to_drop:
                connection.execute(text(f'DROP INDEX IF EXISTS "{index}"'))

    def _get_existing_indexes(self) -> Sequence[str]:
        query = "SELECT indexname FROM pg_indexes"

        with self.engine.connect() as connection:
            return connection.execute(text(query)).scalars().all()

    def _init_indexes(self):
        """Create indexes

        :return:
        """
        indexes = get_indexes(gob_model)
        self._drop_indexes(indexes)
        existing_indexes = self._get_existing_indexes()

        with self.engine.connect() as connection:
            for name, definition in indexes.items():
                if name in existing_indexes:
                    # Don't run CREATE INDEX IF NOT EXISTS query to prevent unnecessary locks
                    continue

                columns = ','.join(definition['columns'])
                index_type = self._get_index_type(definition.get('type'))
                table = definition["table_name"]
                statement = f'CREATE INDEX IF NOT EXISTS "{name}" ON {table} USING {index_type}({columns})'

                if index_type == "GIST":
                    # Create GIST index for valid geometries (used during spatial relate)
                    statement += f" WHERE ST_IsValid({columns})"

                connection.execute(text(statement))

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
        table: Table = self.base.metadata.tables[self.tablename_temp]
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
    def compare_temporary_data(self, mode: ImportMode = ImportMode.FULL) -> Iterator[Sequence[Row]]:
        """ Compare the data in the temporay table to the current state

        The created query compares each model field and returns the tid, last_event
        _hash and if the record should be a ADD, DELETE or MODIFY. CONFIRM records are not
        included in the result, but can be derived from the message

        :return: a iterator of lists containing 25000 rows with tid, hash, last_event and type attributes
        """
        query = queries.get_comparison_query(
            source=self.metadata.source,
            current=self.tablename,
            temporary=self.tablename_temp,
            fields=[FIELD.TID],
            mode=mode
        )
        return self.session.stream_execute(query).partitions(size=25_000)

    @with_session
    def analyze_temporary_table(self):
        """Runs VACUUM ANALYZE on temporary table."""
        query = f"ANALYZE {self.tablename_temp}"
        self.session.bind.execute(text(query), execution_options={"isolation_level": "AUTOCOMMIT"})

    @property
    def DbEvent(self):
        class_ = getattr(self.base.classes, self.EVENTS_TABLE)
        setattr(class_.__table__, "implicit_returning", False)
        return class_

    @property
    def DbEntity(self):
        class_ = getattr(self.base.classes, self.tablename)
        setattr(class_.__table__, "implicit_returning", False)
        return class_

    @property
    def tablename(self) -> str | None:
        if self.metadata:
            return gob_model.get_table_name(self.metadata.catalogue, self.metadata.entity)

    @staticmethod
    def _generate_temp_tablename(metadata) -> str:
        return "_".join(
            ["tmp", gob_model.get_table_name(metadata.catalogue, metadata.entity)[:50], random_string(8)]
        )

    @contextmanager
    def get_session(self, invalidate: bool = False) -> StreamSession:
        """
        Exposes an underlying database session as a managed, not transactional, context.
        Isolation level for this context is 'autocommmit' through sessionmaker.

        :param invalidate: Invalidate and release connection on exiting context.
            Useful in case you want to discard the database session, i.e. to drop temp table
        """

        # Explicitely create Connection (not an Engine) to make it accessible on the `bind` attribute
        connection = self.engine.connect()

        session = self.Session(bind=connection)
        self.session = session

        try:
            yield session
        except Exception as err:
            logger.error(repr(err))
            session.rollback()
            raise
        else:
            session.commit()
        finally:
            if invalidate:
                # release connection to database -> dropping temp tables
                connection.invalidate()

                # make sure temp table is removed from the (base) metadata class var
                if self.metadata and self.tablename_temp in self.base.metadata.tables:
                    self.base.metadata.remove(self.base.metadata.tables[self.tablename_temp])

            session.close()
            self.session = None

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
        with self.engine.connect() as conn:
            return conn.execute(query).scalar() or 0

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
        with self.engine.connect() as conn:
            return conn.execute(query).scalar() or 0

    def get_events_starting_after(self, eventid: int, limit: int = 10_000) -> Sequence[DbEvent]:
        """
        Return chunks of events with eventid starting at `eventid`.
        Example with size=2: ([event1, event2], [event3, event4], [event5])
        This process can take a long time for big collections, keep this in a seperate session.
        Use pagination by eventid instead of streaming results to prevent locking the events table.

        :param eventid: minimal eventid (0 for all)
        :param limit: limit returned result to this size
        :return: Iterator containing lists of events
        """
        Event = self.DbEvent
        stmt = (
            select(Event)
            .where(
                Event.source == self.metadata.source,
                Event.catalogue == self.metadata.catalogue,
                Event.entity == self.metadata.entity,
                Event.eventid > eventid
            )
            .order_by(Event.eventid.asc())
            .limit(limit)
        )

        with StreamSession(self.engine) as session:
            session.expire_on_commit = False  # allows getting object attributes after closing session
            return session.scalars(stmt).all()

    def has_any_event(self, filter_: dict) -> bool:
        """True if any event matches the filter condition

        :return: true is any event has been found given the filter
        """
        query = select(self.DbEvent.eventid).limit(1)

        for key, val in filter_.items():
            query = query.where(getattr(self.DbEvent, key) == val)

        with self.engine.connect() as connection:
            return connection.execute(query).first() is not None

    def has_any_entity(self, key: str = None, value: Any = None) -> bool:
        """Check if any entity exist with the given key-value combination
        When no key-value combination is supplied check for any entity

        :param key: key value, e.g. "_source"
        :param value: value to loop for, e.g. "DIVA"
        :return: True if any entity exists, else False
        """
        query = select(self.DbEntity).limit(1)

        if key and value:
            query = query.where(getattr(self.DbEntity, key) == value)

        with self.engine.connect() as connection:
            return connection.execute(query).first() is not None

    def get_collection_model(self) -> dict | None:
        if self.metadata.catalogue in gob_model:
            return gob_model[self.metadata.catalogue]['collections'].get(self.metadata.entity)

    def get_current_ids(self, exclude_deleted=True) -> Sequence[str]:
        """Overview of entities that are current

        Current id's are evaluated within an application

        :return: a list of ids for the entity that are currently not deleted.
        """
        query = select(self.DbEntity._tid)
        if exclude_deleted:
            query = query.where(self.DbEntity._date_deleted.is_(None))

        with self.engine.connect() as conn:
            return conn.execute(query).scalars().all()

    @with_session
    def get_last_events(self) -> dict[str, int]:
        """Overview of all last applied events for the current collection

        :return: a dict of ids with last_event for the collection
        """
        query = select(self.DbEntity._tid, self.DbEntity._last_event)
        return {row[0]: row[1] for row in self.session.execute(query).all()}

    def get_column_values_for_key_value(self, column: str, key: str, value: Any) -> Sequence[Row]:
        """Gets the distinct values for column within the given source for the given key-value

        Example: get all values for column "identification" with "code" == "A" coming from source "AMSBI"

        :param column: Name of the column for which to return the unique values
        :param key: Name of the column to filter on its value
        :param value: The value to filter the column on
        :return: A list of all unique values for the given combination within the Storage handler source
        """
        attr: Column = getattr(self.DbEntity, column)
        source: Column = getattr(self.DbEntity, FIELD.SOURCE)
        key_col: Column = getattr(self.DbEntity, key)

        stmt = (
            select(attr)
            .distinct()
            .where(
                source == self.metadata.source,
                key_col == value,
                attr.isnot(None)
            )
        )
        with self.engine.connect() as connection:
            return connection.execute(stmt).all()

    def get_last_column_value(self, template: str, column: str) -> Any:
        """Get the "last" value for column with column values that match the template

        Example: Get the last value for "identification" with values that match "036%"

        The last value is defined by the order_by clause.

        :param template: A template string, eg "036%"
        :param column: The column to filter on, eg "identification"
        :return:
        """
        attr: Column = getattr(self.DbEntity, column)
        source: Column = getattr(self.DbEntity, FIELD.SOURCE)

        stmt = (
            select(attr)
            .where(
                source == self.metadata.source,
                attr.like(template)
            )
            .order_by(attr.desc())
            .limit(1)
        )
        with self.engine.connect() as connection:
            return connection.execute(stmt).scalar()

    @with_session
    def get_current_entity(self, entity, with_deleted=False) -> Row | None:
        """Gets current stored version of entity for the given entity.

        If it doesn't exist, returns None

        An entity to retrieve is evaluated within a source
        on the basis of its functional id (_id)

        :param entity: the new version of the entity
        :raises GOBException:
        :return: the stored version of the entity, or None if it doesn't exist
        """
        where = {
            FIELD.TID: entity[FIELD.TID]
        }

        if not with_deleted:
            where[FIELD.DATE_DELETED] = None

        statement = select(self.DbEntity).filter_by(**where)

        try:
            return self.session.execute(statement).one_or_none()
        except MultipleResultsFound:
            filter_str = ','.join([f"{k}={v}" for k, v in where.items()])
            raise GOBException(f"Found multiple rows with filter: {filter_str}")

    @with_session
    def get_entities(self, tids: Iterable[str], with_deleted=False) -> Sequence[DbEntity]:
        """
        Get entities with tid contained in the given list of tid's

        :param tids: ids of the entities to get
        :param with_deleted: boolean denoting if entities that are deleted should be considered (default: False)
        :return:
        """
        values_tid = values(column("_tid", String), name="tids").data([(tid, ) for tid in tids])
        query = select(self.DbEntity).join(values_tid, self.DbEntity._tid == values_tid.c._tid)

        if not with_deleted:
            query = query.where(self.DbEntity._date_deleted.is_(None))

        return self.session.scalars(query).all()

    @with_session
    def add_add_events(self, events):
        # make sure _tid is filled from events.tid, not always present in event.contents
        rows = [event.get_attribute_dict() | {"_last_event": event.id, "_tid": event.tid} for event in events]

        # invoke bulk insert through the Table object, not the mapped class
        self.session.execute(self.DbEntity.__table__.insert(), rows)

    @with_session
    def add_events(self, events):
        """
        Add the given events to the events table

        :param events: the list of events to insert
        :return: None
        """
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
        self.session.execute(self.DbEvent.__table__.insert(), rows)

    @with_session
    def apply_confirms(self, confirms: list[dict], timestamp: str):
        """
        Apply a (BULK)CONFIRM event

        :param confirms: list of confirm data
        :param timestamp: Time to set as last_confirmed
        :return:
        """
        values_tid = \
            values(column("_tid", String), name="tids") \
            .data([(record["_tid"],) for record in confirms])

        stmt = (
            update(self.DbEntity)
            .where(self.DbEntity._tid == values_tid.c._tid)
            .values({CONFIRM.timestamp_field: datetime.datetime.fromisoformat(timestamp)})
            .execution_options(synchronize_session=False)
        )
        self.session.execute(stmt)

    def get_query_value(self, query: str) -> Any:
        """Execute a query and return the result value

        The supplied query needs to resolve to a scalar value

        :param query: Query string
        :return: scalar value result
        """
        with self.engine.connect() as connection:
            return connection.execute(text(query)).scalar()

    def get_source_catalogue_entity_combinations(
        self, catalogue: str, entity: str, source: str = ""
    ) -> Iterator[Row]:
        """Return all unique source / catalogue / entity combinations."""
        # ^@ == startswith
        query = "SELECT tablename FROM pg_tables WHERE schemaname = :events AND tablename ^@ LOWER(:table)"
        params = {"events": self.EVENTS_TABLE, "table": f"{catalogue}_{entity}_{source}"}

        with self.engine.connect() as conn:
            for table in conn.execute(text(query), params).scalars():
                row_qry = f"SELECT catalogue, entity, source FROM {self.EVENTS_TABLE}.{table} LIMIT 1"
                yield from conn.execute(text(row_qry))

    def analyze_table(self):
        """Runs VACUUM ANALYZE on table

        :return:
        """
        with self.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(text(f"VACUUM ANALYZE {self.tablename}"))
