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
from sqlalchemy import create_engine, MetaData, Table, Index
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from gobcore.model import GOBModel
from gobcore.model.metadata import FIXED_COLUMNS, METADATA_COLUMNS
from gobcore.views import GOBViews

from gobupload.config import GOB_DB
from gobupload.storage.db_models import get_column
from gobupload.storage.db_models.event import EVENTS, build_db_event


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

    EVENT_TABLE = "events"
    ALL_TABLES = [EVENT_TABLE] + model.get_model_names()

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
        # Create events table if not yet exists
        if not hasattr(self.base.classes, self.EVENT_TABLE):
            self._init_event()

        # Create model tables
        self._init_entities()

        # Create model views
        self._init_views()

        # refresh reflected base
        self._get_reflected_base()

    def _init_event(self):
        """
        Create the events table
        """
        meta = MetaData(self.engine)

        columns = [get_column(column) for column in EVENTS.items()]
        table = Table(self.EVENT_TABLE, meta, *columns, extend_existing=True)
        table.create(self.engine, checkfirst=True)

    def _init_entities(self):
        """
        Initialize a database table for the gobmodel.
        """

        model = GOBModel()

        for entity_name in model.get_model_names():
            if model.get_model(entity_name)['version'] != "0.1":
                # No migrations defined yet...
                raise ValueError("Unexpected version, please write a generic migration here or migrate the import")

            fields = model.get_model_fields(entity_name)          # the GOB model for the specified entity

            # internal columns
            columns = [get_column(column) for column in FIXED_COLUMNS.items()]
            columns.extend([get_column(column) for column in METADATA_COLUMNS['private'].items()])

            # externally visible columns
            columns.extend([get_column(column) for column in METADATA_COLUMNS['public'].items()])

            # get the entity columns
            data_column_desc = {col: desc['type'] for col, desc in fields.items()}
            columns.extend([get_column(column) for column in data_column_desc.items()])

            # Create an index on source and source_id for performant updates
            index = Index(f"{entity_name}.idx.source_source_id", "_source", "_source_id", unique=True)

            meta = MetaData(self.engine)

            table = Table(entity_name, meta, *columns, index, extend_existing=True)
            table.create(self.engine, checkfirst=True)

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
        return self.base.classes.event

    @property
    def DbEntity(self):
        return getattr(self.base.classes, self.metadata.entity)

    def _drop_table(self, table):
        statement = f"DROP TABLE {table} CASCADE"
        self.engine.execute(statement)

    def drop_tables(self, tables):
        for table in self.ALL_TABLES:
            self._drop_table(table)

    def _truncate_table(self, table):
        statement = f"TRUNCATE TABLE {table}"
        self.engine.execute(statement)

    def truncate_tables(self, tables):
        for table in tables:
            self._truncate_table(table)

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
    def get_current_ids(self):
        """Overview of entities that are current

        :return: a list of ids for the entity that are currently not deleted.
        """
        return self.session.query(self.DbEntity._source_id).filter_by(_source=self.metadata.source,
                                                                      _date_deleted=None).all()

    @with_session
    def get_entity_or_none(self, entity_id, with_deleted=False):
        """Gets an entity. If it doesn't exist, returns None

        :param entity_id: id of the entity to get
        :param with_deleted: boolean denoting if entities that are deleted should be considered (default: False)
        :return:
        """
        entity_query = self.session.query(self.DbEntity).filter_by(_source=self.metadata.source, _source_id=entity_id)
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

    def get_entity_for_update(self, entity_id, source_id, gob_event):
        """Get an entity to work with. Changes to the entity will be persisted on leaving session context

        :param entity_id: id of the entity
        :param source_id: id of the source instance
        :param gob_event: the GOBEvent for which the instance will be used
        :return:
        """

        entity = self.get_entity_or_none(source_id, with_deleted=True)

        if entity is None:
            if not gob_event.is_add_new:
                raise GOBException(f"Trying to '{gob_event.name}' a not existing entity")

            entity = self.DbEntity(_source_id=source_id, _source=self.metadata.source)
            setattr(entity, self.metadata.id_column, entity_id)
            setattr(entity, '_id', entity_id)
            self.session.add(entity)

        if entity._date_deleted is not None and not gob_event.is_add_new:
            raise GOBException(f"Trying to '{gob_event.name}' a deleted entity")

        return entity
