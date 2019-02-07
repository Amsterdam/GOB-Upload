"""Abstraction for the storage that is backing GOB, it is metadata aware, and requires a session in context

Use it like this:

    message = ImportMessage(msg)
    metadata = message.metadata

    storage = GOBStorageHandler(metadata)

    with storage.get_session():
        entity = storage.get_entity_for_update(entity_id, source_id, gob_event)
"""
import functools

from gobcore.exceptions import GOBException
from sqlalchemy import create_engine, Table
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from gobcore.model import GOBModel
from gobcore.model.metadata import PRIVATE_META_FIELDS
from gobcore.model.sa.gob import get_column
from gobcore.typesystem import get_gob_type
from gobcore.views import GOBViews

from gobupload.config import GOB_DB
from gobupload.storage.db_models.event import build_db_event
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

        self.engine = create_engine(URL(**GOB_DB))
        self._get_reflected_base()

        self.session = None

    def _get_reflected_base(self):
        self.base = automap_base()
        self.base.prepare(self.engine, reflect=True)
        self.base.metadata.reflect(bind=self.engine)

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

    def create_temporary_table(self, data):
        """ Create a new temporary table based on the current table for a collection

        Message data is inserted to be compared with the current state

        :param data: the imported data
        :return:
        """
        collection = self.gob_model.get_collection(self.metadata.catalogue, self.metadata.entity)
        table_name = self.gob_model.get_table_name(self.metadata.catalogue, self.metadata.entity)
        new_table_name = table_name + TEMPORARY_TABLE_SUFFIX

        private_fields = ['_source_id', '_hash', '_application']
        fields = [collection['entity_id']]
        # If the collection has state, take begin_geldigheid into account
        if collection.get('has_states'):
            fields.append('begin_geldigheid')

        # Try if the temporary table is already present
        try:
            new_table = self.base.metadata.tables[new_table_name]
        except KeyError:
            columns = [get_column(c, PRIVATE_META_FIELDS[c]) for c in private_fields]
            columns.extend([get_column(c, collection['fields'][c]) for c in fields])
            new_table = Table(new_table_name, self.base.metadata, *columns, extend_existing=True)
            new_table.create(self.engine)
        else:
            # Truncate the table
            self.engine.execute(f"TRUNCATE {new_table_name}")

        # Fill the temporary table
        insert_data = self._fill_temporary_table(data, private_fields, fields)

        self.engine.execute(
            new_table.insert(),
            insert_data
        )

    def _fill_temporary_table(self, data, private_fields, fields):
        """ Fill the temporary table with the data

        :param data: the imported data
        :param private_fields: private fields
        :param fields: fields
        :return: insert_data, a list of dicts
        """
        collection = self.gob_model.get_collection(self.metadata.catalogue, self.metadata.entity)
        # Start inserting the temporary data
        insert_data = []
        for record in data:
            # Add _application to data for comparison
            record['_application'] = self.metadata.application
            row = {}

            for field in private_fields:
                gob_type = get_gob_type(PRIVATE_META_FIELDS[field]['type'])
                row[field] = gob_type.from_value(record[field]).to_db

            for field in fields:
                gob_type = get_gob_type(collection['fields'][field]['type'])
                row[field] = gob_type.from_value(record[field]).to_db

            insert_data.append(row)

            # Remove application from the record
            record.pop('_application', None)
        return insert_data

    def compare_temporary_data(self):
        """ Compare the data in the temporay table to the current state

        The created query compares each model field and returns the source_id, last_event
        _hash and if the record should be a ADD, DELETE or MODIFY. CONFIRM records are not
        included in the result, but can be derived from the message

        :return: a list of dicts with source_id, hash, last_event and type
        """
        collection = self.gob_model.get_collection(self.metadata.catalogue, self.metadata.entity)
        current = self.gob_model.get_table_name(self.metadata.catalogue, self.metadata.entity)
        temporary = current + TEMPORARY_TABLE_SUFFIX

        # Get the result of comparison where data is equal to the current state
        print(queries.get_comparison_query(current, temporary, collection))
        result = self.engine.execute(queries.get_comparison_query(current, temporary, collection)).fetchall()

        # Drop the temporary table
        self.engine.execute(f"DROP TABLE IF EXISTS {temporary}")

        return [dict(row) for row in result]

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

            def __exit__(ctx, type, value, traceback):
                self.session.commit()
                self.session.close()
                self.session = None

        return session_context()

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
            .filter(self.DbEvent.eventid > eventid if eventid else True) \
            .all()

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
    def get_current_ids(self):
        """Overview of entities that are current

        Current id's are evaluated within an application

        :return: a list of ids for the entity that are currently not deleted.
        """
        filter = {
            "_source": self.metadata.source,
            "_application": self.metadata.application,
            "_date_deleted": None
        }
        return self.session.query(self.DbEntity._source_id).filter_by(**filter).all()

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

        If the collection has states (has_states) then the begin_geldigheid needs
        also to be considered

        :param entity: the new version of the entity
        :return: the stored version of the entity, or None if it doesn't exist
        """
        collection = self.gob_model.get_collection(self.metadata.catalogue, self.metadata.entity)

        filter = {
            "_source": self.metadata.source,
            collection["entity_id"]: entity[collection["entity_id"]]
        }
        if collection.get("has_states", False):
            filter["begin_geldigheid"] = entity["begin_geldigheid"]

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
        filter = {
            "_source": self.metadata.source,
            "_source_id": source_id
        }
        entity_query = self.session.query(self.DbEntity).filter_by(**filter)
        if not with_deleted:
            entity_query = entity_query.filter_by(_date_deleted=None)

        return entity_query.one_or_none()

    @with_session
    def add_event_to_storage(self, event):
        """Adds an instance of event to the session, for storage

        :param event: instance of DbEvent to store
        """
        entity = build_db_event(self.DbEvent, event, self.metadata)
        self.session.add(entity)

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

            # Example data (event contents)
            # {
            #     "entity": {"identificatie": "10281154", ... "_last_event": null},
            #     "id_column": "identificatie",
            #      "_last_event": null,
            #      "_source_id": "10281154",
            #      "identificatie": "10281154"
            # }

            collection = self.gob_model.get_collection(self.metadata.catalogue, self.metadata.entity)

            id_column = collection["entity_id"]
            id_value = data["entity"][id_column]
            setattr(entity, id_column, id_value)
            setattr(entity, '_id', id_value)
            setattr(entity, '_version', event.version)
            self.session.add(entity)

        if entity._date_deleted is not None and event.action != "ADD":
            raise GOBException(f"Trying to '{event.action}' a deleted entity")

        return entity

    def get_query_value(self, query):
        """Execute a query and return the result value

        The supplied query needs to resolve to a scalar value

        :param query: Query string
        :return: scalar value result
        """
        return self.engine.execute(query).scalar()
