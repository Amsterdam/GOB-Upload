"""Abstraction for the storage that is backing GOB, it is metadata aware, and requires a session in context.

Use it like this:

    message = ImportMessage(msg)
    metadata = message.metadata

    storage = GOBStorageHandler(metadata)
"""
import functools
import json
import warnings
import random
import string
from typing import Union

from sqlalchemy import create_engine, Table, update, exc as sa_exc
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import MultipleResultsFound

from gobcore.enum import ImportMode
from gobcore.exceptions import GOBException
from gobcore.model.sa.gob import get_column
from gobcore.model.sa.indexes import get_indexes
from gobcore.typesystem import get_gob_type
from gobcore.typesystem.json import GobTypeJSONEncoder
from gobcore.utils import ProgressTicker
from gobcore.events.import_events import CONFIRM

from alembic.runtime import migration
import alembic.config
import alembic.script

from gobupload import gob_model
from gobupload.config import GOB_DB
from gobupload.storage import queries
from gobupload.storage.materialized_views import MaterializedViews


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


class GOBStorageHandler:
    """Metadata aware Storage handler."""
    engine = create_engine(URL.create(**GOB_DB), connect_args={'sslmode': 'require'}, pool_pre_ping=True)
    Session = sessionmaker(autocommit=True,
                           autoflush=False,
                           bind=engine)
    base = None
    added_session_entity_cnt = 0

    @classmethod
    def _set_base(cls, update=False):
        if update or cls.base is None:
            with warnings.catch_warnings():
                # Ignore warnings for unsupported reflection for expression-based indexes
                warnings.simplefilter("ignore", category=sa_exc.SAWarning)

                cls.base = automap_base()
                cls.base.prepare(cls.engine, reflect=True)
                cls.base.metadata.reflect(bind=cls.engine)

    EVENTS_TABLE = "events"
    ALL_TABLES = [EVENTS_TABLE] + gob_model.get_table_names()
    FORCE_FLUSH_PER = 10000

    user_name = f"({GOB_DB['username']}@{GOB_DB['host']}:{GOB_DB['port']})"

    WARNING = 'warning'
    ERROR = 'error'

    config_checks = [
        # (setting, check, message, error/warning)
        ('default_statistics_target', lambda x: int(x) >= 1000, 'should be greater than or equal to 1000', WARNING),
        ('enable_partition_pruning', lambda x: x == 'on', 'should be set to on', ERROR),
        ('constraint_exclusion', lambda x: x == 'on', 'should be set to on', WARNING),
    ]

    def __init__(self, gob_metadata=None):
        """Initialize StorageHandler with gob metadata

        This will create abstractions to entities and events, and initialize storage if necessary

        """
        GOBStorageHandler._set_base()

        self.metadata = gob_metadata
        self.session = None

    def init_storage(
            self,
            force_migrate=False,
            recreate_materialized_views: Union[bool, list] = False,
            raise_on_error: bool = False
    ):
        """Check if the necessary tables (for events, and for the entities in gobmodel) are present
        If not, they are required

        :param force_migrate: Don't wait for any migrations to finish before continuing
        :param recreate_materialized_views: List of mv's to recreate, True for all, False for none
        :param raise_on_error: Exit the application on migration errors
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

            if raise_on_error:
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

    def _get_tmp_table_name(self, table_name):
        # Add a random 3 character string to the table name
        return table_name + "_" + ''.join(random.choice(string.ascii_lowercase) for i in range(3))

    def create_temporary_table(self):
        """ Create a new temporary table based on the current table for a collection

        Message data is inserted to be compared with the current state

        :param data: the imported data
        :return:
        """
        self.collection = self.get_collection_model()
        table_name = gob_model.get_table_name(self.metadata.catalogue, self.metadata.entity)
        tmp_table_name = self._get_tmp_table_name(table_name)

        self.fields = ['_tid', '_source', '_hash']

        # Drop any existing temporary table
        self.drop_temporary_table(tmp_table_name)

        columns = [get_column(c, self.collection['all_fields'][c]) for c in self.fields]
        columns.append(get_gob_type("GOB.JSON").get_column_definition("_original_value"))
        self.tmp_table = Table(tmp_table_name, self.base.metadata, *columns, extend_existing=True)
        self.tmp_table.create(self.engine)

        self.temporary_rows = []
        return tmp_table_name

    def write_temporary_entity(self, entity):
        """
        Writes an entity to the temporary table
        :param entity:
        :return:
        """
        row = {}
        for field in self.fields:
            gob_type = get_gob_type(self.collection['all_fields'][field]['type'])
            if field == '_source':
                row[field] = gob_type.from_value(self.metadata.source).to_db
            else:
                row[field] = gob_type.from_value(entity[field]).to_db
        row["_original_value"] = entity
        self.temporary_rows.append(row)
        self._write_temporary_entities(write_per=10000)

    def _write_temporary_entities(self, write_per=1):
        """
        Writes the temporary entities to the temporary table

        If no arguments are given the write will always take place
        If the write_per argument is specified writes will take place in chunks
        :param write_per:
        :return:
        """
        if len(self.temporary_rows) >= write_per:
            result = self.engine.execute(
                self.tmp_table.insert(),
                self.temporary_rows
            )
            result.close()
            self.temporary_rows = []

    def close_temporary_table(self):
        """
        Writes any left temporary entities to the temporary table
        :return:
        """
        self._write_temporary_entities()

    def compare_temporary_data(self, tmp_table_name, mode=ImportMode.FULL):
        """ Compare the data in the temporay table to the current state

        The created query compares each model field and returns the tid, last_event
        _hash and if the record should be a ADD, DELETE or MODIFY. CONFIRM records are not
        included in the result, but can be derived from the message

        :return: a list of dicts with tid, hash, last_event and type
        """
        current = gob_model.get_table_name(self.metadata.catalogue, self.metadata.entity)

        fields = ["_tid"]
        source = self.metadata.source

        result = None
        try:
            # Get the result of comparison where data is equal to the current state
            result = self.engine.execution_options(stream_results=True)\
                .execute(queries.get_comparison_query(source, current, tmp_table_name, fields, mode))

            for row in result:
                yield dict(row)

        except Exception as e:
            raise e
        finally:
            # Always cleanup
            if result:
                result.close()
            # Drop the temporary table
            self.drop_temporary_table(tmp_table_name)

    def drop_temporary_table(self, tmp_table_name):
        # Drop the temporary table
        self.execute(f"DROP TABLE IF EXISTS {tmp_table_name}")

    @property
    def DbEvent(self):
        return getattr(self.base.classes, self.EVENTS_TABLE)

    @property
    def DbEntity(self):
        return getattr(self.base.classes, self._get_tablename())

    def _get_tablename(self):
        return gob_model.get_table_name(self.metadata.catalogue, self.metadata.entity)

    def _drop_table(self, table):
        statement = f"DROP TABLE IF EXISTS {table} CASCADE"
        self.execute(statement)

    def drop_tables(self):
        for table in self.ALL_TABLES:
            self._drop_table(table)
        # Update the reflected base
        self._set_base(update=True)

    def get_session(self):
        """ Exposes an underlying database session as managed context """

        class session_context:
            def __enter__(ctx):
                self.session = self.Session()
                return self.session

            def __exit__(ctx, exc_type, exc_val, exc_tb):
                if exc_type is not None:
                    self.session.rollback()
                else:
                    self.session.flush()
                self.session.close()
                self.session = None

        return session_context()

    def delete_confirms(self):
        """
        Once (BULK)CONFIRM events have been applied they are deleted

        :return:
        """
        statement = f"""
        DELETE
        FROM {self.EVENTS_TABLE}
        WHERE catalogue = '{self.metadata.catalogue}' AND
              entity = '{self.metadata.entity}' AND
              action IN ('BULKCONFIRM', 'CONFIRM')
        """
        self.execute(statement)

    @with_session
    def get_entity_max_eventid(self):
        """Get the highest last_event property of entity

        :return: The highest last_event
        """
        result = self.session.query(self.DbEntity)\
                     .filter_by(_source=self.metadata.source)\
                     .order_by(self.DbEntity._last_event.desc())\
                     .first()
        return None if result is None else result._last_event

    @with_session
    def get_last_eventid(self):
        """Get the highest last_event property of entity

        :return: The highest last_event
        """
        result = self.session.query(self.DbEvent) \
            .filter_by(source=self.metadata.source, catalogue=self.metadata.catalogue, entity=self.metadata.entity) \
            .order_by(self.DbEvent.eventid.desc())\
            .first()
        return None if result is None else result.eventid

    @with_session
    def get_events_starting_after(self, eventid, count=10000):
        """Return a list of events with eventid starting at eventid

        :return: The list of events
        """
        return self.session.query(self.DbEvent) \
            .filter_by(source=self.metadata.source, catalogue=self.metadata.catalogue, entity=self.metadata.entity) \
            .filter(self.DbEvent.eventid > eventid if eventid else True) \
            .order_by(self.DbEvent.eventid.asc()) \
            .limit(count) \
            .all()

    @with_session
    def has_any_event(self, filter):
        """True if any event matches the filter condition

        :return: true is any event has been found given the filter
        """
        result = self.session.query(self.DbEvent) \
            .filter_by(**filter) \
            .first()
        return result is not None

    @with_session
    def has_any_entity(self, key=None, value=None):
        """Check if any entity exist with the given key-value combination
        When no key-value combination is supplied check for any entity

        :param key: key value, e.g. "_source"
        :param value: value to loop for, e.g. "DIVA"
        :return: True if any entity exists, else False
        """
        if not (key and value):
            return self.session.query(self.DbEntity).count() > 0
        return self.session.query(self.DbEntity).filter_by(**{key: value}).count() > 0

    def get_collection_model(self):
        if self.metadata.catalogue in gob_model:
            return gob_model[self.metadata.catalogue]['collections'].get(self.metadata.entity)
        return None

    @with_session
    def get_current_ids(self, exclude_deleted=True):
        """Overview of entities that are current

        Current id's are evaluated within an application

        :return: a list of ids for the entity that are currently not deleted.
        """
        if exclude_deleted:
            filter["_date_deleted"]: None
        return self.session.query(self.DbEntity._tid).all()

    @with_session
    def get_last_events(self):
        """Overview of all last applied events for the current collection

        :return: a dict of ids with last_event for the collection
        """
        result = self.session.query(self.DbEntity._tid, self.DbEntity._last_event).all()
        return {row._tid: row._last_event for row in result}

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
    def get_entities(self, tids, with_deleted=False):
        """
        Get entities with tid contained in the given list of tid's

        :param tids: ids of the entities to get
        :param with_deleted: boolean denoting if entities that are deleted should be considered (default: False)
        :return:
        """
        entity_query = self.session.query(self.DbEntity)
        if not with_deleted:
            entity_query = entity_query.filter_by(_date_deleted=None)

        attr = getattr(self.DbEntity, "_tid")
        return entity_query.filter(attr.in_(tids)).all()

    def bulk_add_entities(self, events):
        """Adds all applied ADD events to the storage

        :param events: list of gob events
        """
        insert_data = []
        progress = ProgressTicker("Bulk add entities", 10000)
        while events:
            progress.tick()

            event = events.pop(0)
            entity = event.get_attribute_dict()
            # Set the the _last_event
            entity['_last_event'] = event.id
            insert_data.append(entity)
        table = self.DbEntity.__table__
        self.bulk_insert(table, insert_data)

    @with_session
    def add_add_events(self, events):
        rows = []
        for event in events:
            entity = event.get_attribute_dict()
            # Set the the _last_event
            entity['_last_event'] = event.id
            rows.append(entity)
        table = self.DbEntity.__table__
        self.session.execute(table.insert(), rows)

    @with_session
    def add_events(self, events):
        """
        Add the given events to the events table

        :param events: the list of events to insert
        :return: None
        """
        def escape(value):
            return value.replace("'", "''").replace("%", "%%") if isinstance(value, str) else value

        def to_json(data):
            """
            Convert the data dictionary to a JSON string that can be inserted in the events table.

            :param data: dictionary
            :return: the JSON string suitably quoted to be used as a string literal in an SQL statement string
            """
            return json.dumps(data, cls=GobTypeJSONEncoder)

        values = ",".join([f"""
(
    '{ self.metadata.timestamp }',
    '{ self.metadata.catalogue }',
    '{ self.metadata.entity }',
    '{ event['version'] }',
    '{ event['event'] }',
    '{ self.metadata.source }',
    '{ escape(event['data'].get('_source_id')) }',
    '{ escape(to_json(event['data'])) }',
    '{ self.metadata.application }',
    '{ escape(event['data']['_tid']) }'
)""" for event in events])

        # INSERT INTO events (...) VALUES (...)[, (...), ...]
        statement = f"""
INSERT INTO
    "{ self.EVENTS_TABLE }"
(
    "timestamp",
    catalogue,
    entity,
    "version",
    "action",
    "source",
    source_id,
    contents,
    application,
    tid
)
VALUES {values}"""
        self.execute(statement)

    def bulk_update_confirms(self, event, eventid):
        """ Confirm entities in bulk

        Takes a BULKCONFIRM event and updates all tids with the bulkconfirm timestamp

        The _last_event is not updated for (BULK)CONFIRM events

        :param event: the BULKCONFIRM event
        :param eventid: the id of the event to store as _last_event
        :return:
        """
        self.apply_confirms(event._data['confirms'], event._metadata.timestamp)

    def apply_confirms(self, confirms, timestamp):
        """
        Apply a (BULK)CONFIRM event

        :param confirms: list of confirm data
        :param timestamp: Time to set as last_confirmed
        :return:
        """
        tids = [record['_tid'] for record in confirms]
        stmt = update(self.DbEntity).where(self.DbEntity._tid.in_(tids)).\
            values({CONFIRM.timestamp_field: timestamp})
        self.execute(stmt)

    def bulk_insert(self, table, insert_data):
        """ A generic bulk insert function

        Takes a list of dictionaries and the database table to insert into

        :param table: the table to insert into
        :param insert_data: the data to insert
        :return:
        """
        result = self.engine.execute(
            table.insert(),
            insert_data
        )
        result.close()

    @with_session
    def _flush_entities(self):
        if self.added_session_entity_cnt >= self.FORCE_FLUSH_PER:
            self.force_flush_entities()

    @with_session
    def force_flush_entities(self):
        self.session.flush()
        self.added_session_entity_cnt = 0

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

    def get_source_catalogue_entity_combinations(self, **kwargs):
        stmt = "SELECT DISTINCT source, catalogue, entity FROM events"
        where = {k: v for k, v in kwargs.items() if v}
        if where:
            stmt += " WHERE " + "AND ".join([f"{k} = '{v}'" for k, v in where.items()])
        with self.get_session() as session:
            return [combination for combination in session.execute(stmt)]

    def analyze_table(self):
        """Runs VACUUM ANALYZE on table

        :return:
        """
        # Create separate connection and start with COMMIT to be outside of transaction context, otherwise VACUUM won't
        # work.
        connection = self.engine.connect()
        connection.execute("COMMIT")
        connection.execute(f"VACUUM ANALYZE {self._get_tablename()}")
        connection.close()
