import functools

from gobcore.exceptions import GOBException
from sqlalchemy import create_engine, MetaData, Table, Index
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from gobuploadservice.config import GOB_DB
from gobuploadservice.storage.db_models import get_column
from gobuploadservice.storage.db_models.event import EVENTS, build_db_event
from gobuploadservice.storage.db_models.metadata import FIXED_COLUMNS, METADATA_COLUMNS


def with_session(func):
    """Decorator for methods that require the session in the context """
    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        self = args[0]
        if self.session is None:
            raise GOBException("No current session")
        return func(*args, **kwargs)
    return wrapper_decorator


class GOBStorageHandler():
    """Metadata aware Storage handler """
    EVENT_TABLE = "event"

    def __init__(self, gob_metadata):
        """Initialize DBHandler with gob metadata

        This will create abstractions to entities and events, and initialize storage if necessary

        """
        self.metadata = gob_metadata
        self.engine = create_engine(URL(**GOB_DB))
        self._get_reflected_base()
        self._init_storage()
        self.session = None

    def _get_reflected_base(self):
        self.base = automap_base()
        self.base.prepare(self.engine, reflect=True)
        self.base.metadata.reflect(bind=self.engine)

    def _init_storage(self):
        if not hasattr(self.base.classes, self.metadata.entity):
            # Create events table if not yet exists
            if not hasattr(self.base.classes, self.EVENT_TABLE):
                self._init_event()

            # create table
            self._init_entity()

            # refresh reflected base
            self._get_reflected_base()

        if self.metadata.version != "0.1":
            # No migrations defined yet...
            raise ValueError("Unexpected version, please write a generic migration here of migrate the import")

    def _init_event(self):
        """
        Create the events table
        """
        meta = MetaData(self.engine)

        columns = [get_column(column) for column in EVENTS.items()]
        table = Table(self.EVENT_TABLE, meta, *columns, extend_existing=True)
        table.create(self.engine, checkfirst=True)

    def _init_entity(self):
        """
        Initialize a database table for the given metadata.
        """

        table_name = self.metadata.entity    # e.g. meetbouten
        model = self.metadata.model          # the GOB model for the specified entity

        # internal columns
        columns = [get_column(column) for column in FIXED_COLUMNS.items()]
        columns.extend([get_column(column) for column in METADATA_COLUMNS['private'].items()])

        # externally visible columns
        columns.extend([get_column(column) for column in METADATA_COLUMNS['public'].items()])

        # get the entity columns
        data_column_desc = {col: desc['type'] for col, desc in model.items()}
        columns.extend([get_column(column) for column in data_column_desc.items()])

        # Create an index on source and source_id for performant updates
        index = Index(f"{table_name}.idx.source_source_id", "_source", "_source_id", unique=True)

        meta = MetaData(self.engine)

        table = Table(table_name, meta, *columns, index, extend_existing=True)
        table.create(self.engine, checkfirst=True)

    @property
    def DbEvent(self):
        return self.base.classes.event

    @property
    def DbEntity(self):
        return getattr(self.base.classes, self.metadata.entity)

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
        return self.session.query(self.DbEntity._source_id).filter_by(_source=self.metadata.source,
                                                                      _date_deleted=None).all()

    @with_session
    def get_entity_or_none(self, entity_id, with_deleted=False):
        entity_query = self.session.query(self.DbEntity).filter_by(_source=self.metadata.source, _source_id=entity_id)
        if not with_deleted:
            entity_query = entity_query.filter_by(_date_deleted=None)

        return entity_query.one_or_none()

    @with_session
    def add_event_to_db(self, event):
        entity = build_db_event(self.DbEvent, event, self.metadata)
        self.session.add(entity)

    def get_entity_for_update(self, entity_id, source_id, gob_event):
        entity = self.get_entity_or_none(source_id, with_deleted=True)

        if entity is None:
            if not gob_event.is_add_new:
                raise GOBException(f"Trying to '{gob_event.name}' a not existing entity")

            entity = self.DbEntity(_source_id=source_id, _source=self.metadata.source)
            setattr(entity, self.metadata.id_column, entity_id)
            self.session.add(entity)

        if entity._date_deleted is not None and not gob_event.is_add_new:
            raise GOBException(f"Trying to '{gob_event.name}' a deleted entity")

        return entity
