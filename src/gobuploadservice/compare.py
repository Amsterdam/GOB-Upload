"""Compare new data with the existing data

Derive Add, Change, Delete and Confirm events by comparing a full set of new data against the full set of current data

Todo: Event, action and mutation are used for the same subject. Use one name to improve maintainability.

"""
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import Session

from gobcore.events import get_event_for
from gobcore.events.import_message import ImportMessage

from gobcore.typesystem import get_modifications

from gobuploadservice import print_report
from gobuploadservice.config import GOB_DB
from gobuploadservice.storage.init_storage import init_storage
from gobuploadservice.storage.util import get_reflected_base

"""SQLAlchemy engine that encapsulates the database"""
engine = create_engine(URL(**GOB_DB))
Base = get_reflected_base(engine)


def compare(msg):
    """Compare new data in msg (contents) with the current data

    :param msg: The new data, including header and summary
    :return: result message
    """

    # Parse the message header
    message = ImportMessage(msg)
    metadata = message.metadata

    # Todo: this should be done elsewhere
    base = init_storage(metadata, engine, Base)

    # Read new content into dictionary
    new_entities = {data['_source_id']: data for data in msg["contents"]}

    # Get the table where the current entities are stored
    DbEntity = getattr(base.classes, metadata.entity)

    # Get all current non-deleted ids for this source
    session = Session(engine)
    current_ids = session.query(DbEntity._source_id).filter_by(_source=metadata.source,
                                                               _date_deleted=None).all()

    # find deletes by comparing current ids to new entities
    # if a current_id is not found in the new_entities it is interpreted as a deletion
    deleted = {current._source_id: None for current in current_ids if current._source_id not in new_entities}

    all = {**new_entities, **deleted}
    # Make sure there are no id's (keys) that are present in both
    assert len(all) == len(new_entities) + len(deleted)

    events = []
    for entity_id, data in all.items():
        entity = session.query(DbEntity).filter_by(_source=metadata.source,
                                                   _source_id=entity_id,
                                                   _date_deleted=None).one_or_none()

        modifications = get_modifications(entity, data, metadata.model)
        event = get_event_for(entity, data, metadata, modifications)
        events.append(event)

    session.close()

    print_report(events)

    # Return the result without log.
    return ImportMessage.create_import_message(metadata.as_header, None, events)
