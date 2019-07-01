"""Abstraction for the storage that is backing GOB, it is metadata aware, and requires a session in context

Use it like this:

    message = ImportMessage(msg)
    metadata = message.metadata

    storage = GOBStorageHandler(metadata)

    with storage.get_session():
        entity = storage.get_entity_for_update(entity_id, source_id, gob_event)
"""
import copy
import functools
import json

from sqlalchemy import create_engine, Table, update
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from gobcore.exceptions import GOBException
from gobcore.model import GOBModel
from gobcore.model.sa.gob import get_column, indexes
from gobcore.typesystem import get_gob_type
from gobcore.typesystem.json import GobTypeJSONEncoder
from gobcore.views import GOBViews
from gobcore.utils import ProgressTicker

from gobupload.config import GOB_DB
from gobupload.storage import queries

import alembic.config


TEMPORARY_TABLE_SUFFIX = '_tmp'


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


class GOBStorageHandler():
    """Metadata aware Storage handler """
    model = GOBModel()

    EVENTS_TABLE = "events"
    ALL_TABLES = [EVENTS_TABLE] + model.get_table_names()

    user_name = f"({GOB_DB['username']}@{GOB_DB['host']}:{GOB_DB['port']})"

    def __init__(self, gob_metadata=None):
        """Initialize StorageHandler with gob metadata

        This will create abstractions to entities and events, and initialize storage if necessary

        """
        self.metadata = gob_metadata
        self.gob_model = GOBModel()

        self.engine = self.__class__.get_engine()
        self._get_reflected_base()

        self.session = None

    def _get_reflected_base(self):
        self.base = automap_base()
        self.base.prepare(self.engine, reflect=True)
        self.base.metadata.reflect(bind=self.engine)

    @staticmethod
    def get_engine():
        return create_engine(URL(**GOB_DB))

    def init_storage(self):
        """Check if the necessary tables (for events, and for the entities in gobmodel) are present
        If not, they are required
        """

        # Database migrations are handled by alembic
        # alembic upgrade head
        alembicArgs = [
            '--raiseerr',
            'upgrade', 'head',
        ]
        alembic.config.main(argv=alembicArgs)

        # Create model views
        self._init_views()

        # refresh reflected base
        self._get_reflected_base()

        # Create necessary indexes
        self._init_indexes()

    def _init_views(self):
        """
        Initialize the views for the gobviews.
        """

        views = GOBViews()
        for catalog in views.get_catalogs():
            for entity in views.get_entities(catalog):
                for view_name, view in views.get_views(catalog, entity).items():
                    if view['version'] != "0.1":
                        # No migrations defined yet...
                        raise ValueError(
                            "Unexpected version, please write a generic migration here or migrate the view"
                        )

                    self._create_view(f"{catalog}_{entity}_{view_name}", "\n".join(view['query']))

    def _create_view(self, name, definition):
        """Create view

        Use DROP + CREATE because CREATE OR REPLACE raised an exception for some views

        :param name: Name of the view
        :param definition: Definition (SQL)
        :return: None
        """
        statements = [f"DROP VIEW IF EXISTS {name} CASCADE", f"CREATE VIEW {name} AS {definition}"]
        for statement in statements:
            self.engine.execute(statement)

    def _get_index_type(self, type: str) -> str:
        if type == "geo":
            return "GIST"
        elif type == "json":
            return "GIN"
        else:
            return "BTREE"

    def _init_indexes(self):
        """Create indexes

        :return:
        """

        for name, definition in indexes.items():
            columns = ','.join(definition['columns'])
            index_type = self._get_index_type(definition.get('type'))
            statement = f"CREATE INDEX IF NOT EXISTS \"{name}\" " \
                f"ON {definition['table_name']} USING {index_type}({columns})"

            try:
                self.engine.execute(statement)
            except OperationalError as e:
                print(f"ERROR: Index {name} failed")

    def create_temporary_table(self):
        """ Create a new temporary table based on the current table for a collection

        Message data is inserted to be compared with the current state

        :param data: the imported data
        :return:
        """
        self.collection = self.gob_model.get_collection(self.metadata.catalogue, self.metadata.entity)
        table_name = self.gob_model.get_table_name(self.metadata.catalogue, self.metadata.entity)
        tmp_table_name = table_name + TEMPORARY_TABLE_SUFFIX

        self.fields = self.gob_model.get_functional_key_fields(self.metadata.catalogue, self.metadata.entity)
        self.fields.extend(['_source_id', '_hash'])

        # Try if the temporary table is already present
        try:
            self.tmp_table = self.base.metadata.tables[tmp_table_name]
        except KeyError:
            columns = [get_column(c, self.collection['all_fields'][c]) for c in self.fields]
            columns.append(get_gob_type("GOB.JSON").get_column_definition("_original_value"))
            self.tmp_table = Table(tmp_table_name, self.base.metadata, *columns, extend_existing=True)
            self.tmp_table.create(self.engine)
        else:
            # Truncate the table
            self.engine.execute(f"TRUNCATE {tmp_table_name}")

        self.temporary_rows = []

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
            self.engine.execute(
                self.tmp_table.insert(),
                self.temporary_rows
            )
            self.temporary_rows = []

    def close_temporary_table(self):
        """
        Writes any left temporary entities to the temporary table
        :return:
        """
        self._write_temporary_entities()

    def compare_temporary_data(self):
        """ Compare the data in the temporay table to the current state

        The created query compares each model field and returns the source_id, last_event
        _hash and if the record should be a ADD, DELETE or MODIFY. CONFIRM records are not
        included in the result, but can be derived from the message

        :return: a list of dicts with source_id, hash, last_event and type
        """
        current = self.gob_model.get_table_name(self.metadata.catalogue, self.metadata.entity)
        temporary = current + TEMPORARY_TABLE_SUFFIX

        fields = self.gob_model.get_functional_key_fields(self.metadata.catalogue, self.metadata.entity)

        # Get the result of comparison where data is equal to the current state
        result = self.engine.execute(queries.get_comparison_query(current, temporary, fields))

        for row in result:
            yield dict(row)

        # Drop the temporary table
        self.engine.execute(f"DROP TABLE IF EXISTS {temporary}")

    @property
    def DbEvent(self):
        return getattr(self.base.classes, self.EVENTS_TABLE)

    @property
    def DbEntity(self):
        table_name = self.gob_model.get_table_name(self.metadata.catalogue, self.metadata.entity)
        return getattr(self.base.classes, table_name)

    def _drop_table(self, table):
        statement = f"DROP TABLE IF EXISTS {table} CASCADE"
        self.engine.execute(statement)

    def drop_tables(self):
        for table in self.ALL_TABLES:
            self._drop_table(table)
        # Update the reflected base
        self._get_reflected_base()

    def get_session(self):
        """ Exposes an underlying database session as managed context """

        class session_context:
            def __enter__(ctx):
                self.session = Session(self.engine)
                return self.session

            def __exit__(ctx, exc_type, exc_val, exc_tb):
                if exc_type is not None:
                    self.session.rollback()
                else:
                    self.session.flush()
                    self.session.commit()
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
        self.engine.execute(statement)

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
            .order_by(self.DbEvent.eventid.desc()) \
            .first()
        return None if result is None else result.eventid

    @with_session
    def get_events_starting_after(self, eventid):
        """Return a list of events with eventid starting at eventid

        :return: The list of events
        """
        return self.session.query(self.DbEvent) \
            .filter_by(source=self.metadata.source, catalogue=self.metadata.catalogue, entity=self.metadata.entity) \
            .filter(self.DbEvent.eventid > eventid if eventid else True)

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
        return self.gob_model.get_collection(self.metadata.catalogue, self.metadata.entity)

    @with_session
    def get_current_ids(self, exclude_deleted=True):
        """Overview of entities that are current

        Current id's are evaluated within an application

        :return: a list of ids for the entity that are currently not deleted.
        """
        filter = {
            "_source": self.metadata.source,
            "_application": self.metadata.application
        }
        if exclude_deleted:
            filter["_date_deleted"]: None
        return self.session.query(self.DbEntity._source_id).filter_by(**filter).all()

    @with_session
    def get_last_events(self):
        """Overview of all last applied events for the current collection

        :return: a dict of ids with last_event for the collection
        """
        filter = {
            "_source": self.metadata.source
        }
        result = self.session.query(self.DbEntity._source_id, self.DbEntity._last_event).filter_by(**filter).all()
        return {row._source_id: row._last_event for row in result}

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
        :return: the stored version of the entity, or None if it doesn't exist
        """
        fields = self.gob_model.get_functional_key_fields(self.metadata.catalogue, self.metadata.entity)
        value = {
            **entity,
            "_source": self.metadata.source
        }
        filter = {field: value[field] for field in fields}

        entity_query = self.session.query(self.DbEntity).filter_by(**filter)
        if not with_deleted:
            entity_query = entity_query.filter_by(_date_deleted=None)

        return entity_query.one_or_none()

    @with_session
    def get_entity_or_none(self, source_id, with_deleted=False):
        """Gets an entity. If it doesn't exist, returns None

        :param source_id: id of the entity to get
        :param with_deleted: boolean denoting if entities that are deleted should be considered (default: False)
        :return:
        """
        fields = self.gob_model.get_technical_key_fields(self.metadata.catalogue, self.metadata.entity)
        value = {
            "_source": self.metadata.source,
            "_source_id": source_id
        }
        filter = {field: value[field] for field in fields}

        entity_query = self.session.query(self.DbEntity).filter_by(**filter)
        if not with_deleted:
            entity_query = entity_query.filter_by(_date_deleted=None)

        return entity_query.one_or_none()

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

    def add_events(self, events):
        rows = [{
            'timestamp': self.metadata.timestamp,
            'catalogue': self.metadata.catalogue,
            'entity': self.metadata.entity,
            'version': self.metadata.version,
            'action': event['event'],
            'source': self.metadata.source,
            'application': self.metadata.application,
            'source_id': event['data'].get('_source_id'),
            'contents': json.dumps(copy.deepcopy(event['data']), cls=GobTypeJSONEncoder),
        } for event in events]
        table = self.base.metadata.tables[self.EVENTS_TABLE]
        self.session.execute(table.insert(), rows)

    def bulk_add_events(self, events):
        """Adds all ADD events to the session, for storage

        :param events: list of events
        """
        # Create the ADD event insert list
        insert_data = []
        progress = ProgressTicker("Bulk add events", 10000)
        while events:
            progress.tick()

            event = events.pop(0)
            row = {
                'timestamp': self.metadata.timestamp,
                'catalogue': self.metadata.catalogue,
                'entity': self.metadata.entity,
                'version': self.metadata.version,
                'action': event['event'],
                'source': self.metadata.source,
                'application': self.metadata.application,
                'source_id': event['data'].get('_source_id'),
                'contents': json.dumps(copy.deepcopy(event['data']), cls=GobTypeJSONEncoder),
            }
            insert_data.append(row)
        table = self.base.metadata.tables[self.EVENTS_TABLE]

        self.bulk_insert(table, insert_data)

    def bulk_update_confirms(self, event, eventid):
        """ Confirm entities in bulk

        Takes a BULKCONFIRM event and updates all source_ids with the bulkconfirm timestamp

        The _last_event is not updated for (BULK)CONFIRM events

        :param event: the BULKCONFIRM event
        :param eventid: the id of the event to store as _last_event
        :return:
        """
        source_ids = [record['_source_id'] for record in event._data['confirms']]
        stmt = update(self.DbEntity).where(self.DbEntity._source_id.in_(source_ids)).\
            values({event.timestamp_field: event._metadata.timestamp})
        self.engine.execute(stmt)

    def bulk_insert(self, table, insert_data):
        """ A generic bulk insert function

        Takes a list of dictionaries and the database table to insert into

        :param table: the table to insert into
        :param insert_data: the data to insert
        :return:
        """
        self.engine.execute(
            table.insert(),
            insert_data
        )

    def get_entity_for_update(self, event, data):
        """Get an entity to work with. Changes to the entity will be persisted on leaving session context

        :param entity_id: id of the entity
        :param source_id: id of the source instance
        :param gob_event: the GOBEvent for which the instance will be used
        :return:
        """

        entity = self.get_entity_or_none(data["_entity_source_id"], with_deleted=True)

        if entity is None:
            if event.action != "ADD":
                raise GOBException(f"Trying to '{event.action}' a not existing entity")

            # Create an empty entity for the sepcified source and source_id
            entity = self.DbEntity(_source=self.metadata.source, _source_id=event.source_id)

            self.session.add(entity)

        if entity._date_deleted is not None and event.action != "ADD":
            raise GOBException(f"Trying to '{event.action}' a deleted entity")

        return entity

    def execute(self, statement):
        return self.engine.execute(statement)

    def get_query_value(self, query):
        """Execute a query and return the result value

        The supplied query needs to resolve to a scalar value

        :param query: Query string
        :return: scalar value result
        """
        return self.engine.execute(query).scalar()
