"""Compare new data with the existing data

Derive Add, Change, Delete and Confirm actions by comparing a full set of new data against the full set of current data

"""
import datetime
from decimal import Decimal

from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base

from gobuploadservice.config import GOB_DB
from gobuploadservice.gob_config import GOBHeader

"""SQLAlchemy engine that encapsulates the database"""
engine = create_engine(URL(**GOB_DB))

# prepare base for autoreflecting existing tables
Base = automap_base()
Base.prepare(engine, reflect=True)
Base.metadata.reflect(bind=engine)


def compare(msg):
    """Compare new data in msg (contents) with the current data

    :param msg: The new data, including header and summary
    :return: result message
    """

    # Parse the message header
    metadata = GOBHeader(msg)

    # Do any migrations if the data is behind in version
    if metadata.version != "0.1":
        # No migrations defined yet...
        raise ValueError("Unexpected version, please write a generic migration here of migrate the import")

    # Todo: I propose kicking of another message, with another handler, if migration (or init of datatype) is needed

    # Get the table where the current entities are stored
    Entity = getattr(Base.classes, metadata.entity)
    session = Session(engine)

    # Get all current non-deleted ids for this source
    current_ids = session.query(Entity._source_id).filter_by(_source=metadata.source, _date_deleted=None).all()

    # Read new content into dictionary
    new_entities = {data['_source_id']: data for data in msg["contents"]}

    # find deletes by comparing current ids to new entities:
    deleted_ids = [current._source_id for current in current_ids if current._source_id not in new_entities]

    mutations = []
    # create delete mutations
    for deleted_id in deleted_ids:
        entity = session.query(Entity).filter_by(_source=metadata.source, _source_id=deleted_id).one()
        mutations.append(_create_delete_mutation(entity, metadata))

    # create other mutations
    for new_id, new_entity in new_entities.items():
        entity = session.query(Entity).filter_by(_source=metadata.source, _source_id=new_id).one_or_none()
        mutations.append(_create_confirm_modify_or_delete(entity, metadata, new_entity))

    session.close()

    _print_report(mutations)

    # Return the result
    return {
        "header": metadata.as_header,
        "summary": None,  # No log, metrics and qa indicators for now
        "contents": mutations,
    }


def _create_confirm_modify_or_delete(entity, metadata, new_entity):
    if entity is not None:
        if _is_equal(entity, new_entity):
            return _create_confirmed_mutation(entity, metadata)
        else:
            return _create_modified_mutation(entity, new_entity, metadata)
    else:
        return _create_add_mutation(new_entity, metadata)


def _is_equal(entity, new_entity):
    # this should be based on gob-model description, however for now:
    # Skip all derived and meta data by filtering on not starting with "_"
    attributes_to_check = [key for key, v in new_entity.items() if not key.startswith('_')]

    for attribute in attributes_to_check:
        if not _is_attr_equal(new_entity[attribute], getattr(entity, attribute)):
            return False

    return True


def _is_attr_equal(new_value, stored_value):
    """Determine if a new value is equal to a already stored value

    Values may be stored in a different format than the new data. Convert the data where required

    Todo: this should be generic functionality of GOB-datatypes

    :param new_value: The new value
    :param stored_value: The currently stored value
    :return: True if both values compare equal
    """
    if isinstance(stored_value, datetime.date):
        return new_value == stored_value.strftime("%Y-%m-%d")
    elif isinstance(stored_value, Decimal):
        return new_value == str(stored_value)
    elif isinstance(stored_value, bool):
        return new_value == str(stored_value)
    else:
        return new_value == stored_value


def _create_delete_mutation(entity, metadata):
    contents = {"_source_id": entity._source_id, metadata.id_column: getattr(entity, metadata.id_column)}
    return {"action": "DELETED", "contents": contents}


def _create_confirmed_mutation(entity, metadata):
    contents = {"_source_id": entity._source_id, metadata.id_column: getattr(entity, metadata.id_column)}
    return {"action": "CONFIRMED", "contents": contents}


def _create_add_mutation(new_entity, metadata):
    ids = {"_source_id": new_entity['_source_id'], metadata.id_column: new_entity[metadata.id_column]}
    return {"action": "ADD", "contents": {**new_entity, **ids}}


def _create_modified_mutation(entity, new_entity, metadata):
    ids = {"_source_id": new_entity['_source_id'], metadata.id_column: new_entity[metadata.id_column]}
    modifications = []

    attributes_to_check = [key for key, v in new_entity.items() if not key.startswith('_')]

    for attribute in attributes_to_check:
        if not _is_attr_equal(new_entity[attribute], getattr(entity, attribute)):
            modifications.append({
                "key": attribute,
                "new_value": new_entity[attribute],
                "old_value": getattr(entity, attribute)
            })

    return {"action": "MODIFIED", "contents": dict(modifications=modifications, **ids)}


def _print_report(mutations):
    # Print a simple report
    print(f"Aantal mutaties: {len(mutations)}")
    for action in ["ADD", "MODIFIED", "CONFIRMED", "DELETED"]:
        actions = [mutation for mutation in mutations if mutation['action'] == action]
        if len(actions) > 0:
            print(f"- {action}: {len(actions)}")
