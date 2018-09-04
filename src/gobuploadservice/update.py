"""Update the current data

Process events and apply the event on the current state of the entity
"""
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import Session

from gobcore.events.import_message import ImportMessage
from gobcore.events import GobEvent

from gobuploadservice.storage.db_models.event import create_event

from gobuploadservice import print_report
from gobuploadservice.config import GOB_DB
from gobuploadservice.storage.init_storage import init_storage
from gobuploadservice.storage.util import get_reflected_base

"""SQLAlchemy engine that encapsulates the database"""
engine = create_engine(URL(**GOB_DB))
Base = get_reflected_base(engine)


def full_update(msg):
    """Apply the events on the current dataset

    :param msg: the result of the application of the events
    :return:
    """
    # Interpret the message header
    message = ImportMessage(msg)
    metadata = message.metadata

    # Todo: this should be done elsewhere
    base = init_storage(metadata, engine, Base)

    # Reflect on the database to get an Object-mapping to the entity
    DbEvent = base.classes.event
    DbEntity = getattr(base.classes, metadata.entity)

    session = Session(engine)

    for event in message.contents:
        # Store the event in the database
        session.add(create_event(DbEvent, event, metadata))

        # Get the gob_event
        gob_event = GobEvent(event, metadata)

        # read and remove relevant id's (requires `compare` to put them in)
        entity_id, source_id = gob_event.pop_ids()

        # Updates on entities are uniquely identified by the source and source_id
        entity = session.query(DbEntity).filter_by(_source=metadata.source,
                                                   _source_id=source_id).one_or_none()

        # Make sure an entity is available when the event requires an addition.
        if gob_event.is_add_new:
            if entity is None:
                # create a new Entity
                entity = DbEntity(_source_id=source_id, _source=metadata.source)
                setattr(entity, metadata.id_column, entity_id)
                session.add(entity)

            entity._date_deleted = None

        # todo: create meaningfull exceptions in GOB-Core
        assert entity._date_deleted is None

        # apply the event on the entity
        gob_event.apply_to(entity)

    # Todo: think about transactional integrity, here the db update can be succesfull, while the report back can fail,
    #       triggering a requeue.

    session.commit()
    session.close()

    print_report(message.contents)

    # Return the result message, with no log, no contents
    return ImportMessage.create_import_message(metadata.as_header, None, None)
