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
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from gobcore.model import GOBModel
from gobcore.views import GOBViews

from gobupload.config import GOB_DB
from gobupload.storage.db_models.event import build_db_event

import alembic.config


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
        statement = f"CREATE OR REPLACE VIEW {name} AS {definition}"
        self.engine.execute(statement)

    @property
    def DbEvent(self):
        return getattr(self.base.classes, self.EVENTS_TABLE)

    @property
    def DbEntity(self):
        table_name = GOBModel().get_table_name(self.metadata.catalogue, self.metadata.entity)
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
    def has_any_entity(self, key, value):
        """Check if any entity exist with the given key-value combination

        :param key: key value, e.g. "_source"
        :param value: value to loop for, e.g. "DIVA"
        :return: True if any entity exists, else False
        """
        return self.session.query(self.DbEntity).filter_by(**{key: value}).count() > 0

    @with_session
    def get_current_ids(self):
        """Overview of entities that are current

        :return: a list of ids for the entity that are currently not deleted.
        """
        return self.session.query(self.DbEntity._source_id).filter_by(_source=self.metadata.source,
                                                                      _date_deleted=None).all()

    @with_session
    def get_entity_or_none(self, source_id, with_deleted=False):
        """Gets an entity. If it doesn't exist, returns None

        :param source_id: id of the entity to get
        :param with_deleted: boolean denoting if entities that are deleted should be considered (default: False)
        :return:
        """
        entity_query = self.session.query(self.DbEntity).filter_by(_source=self.metadata.source, _source_id=source_id)
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

        entity = self.get_entity_or_none(event.source_id, with_deleted=True)

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

            collection = GOBModel().get_collection(self.metadata.catalogue, self.metadata.entity)

            id_column = collection["entity_id"]
            id_value = data["entity"][id_column]
            setattr(entity, id_column, id_value)
            setattr(entity, '_id', id_value)
            setattr(entity, '_version', event.version)
            self.session.add(entity)

        if entity._date_deleted is not None and event.action != "ADD":
            raise GOBException(f"Trying to '{event.action}' a deleted entity")

        return entity
