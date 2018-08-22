"""Compare new data with the existing data

Derive Add, Change, Delete and Confirm events by comparing a full set of new data against the full set of current data

Todo: Event, action and mutation are used for the same subject. Use one name to improve maintainability.

"""
import datetime
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import Session

from gobcore.events import GOB
from gobcore.events import GOB_EVENTS
from gobcore.message_broker.message import GOBHeader

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
    metadata = GOBHeader(msg)

    # Initialize the storage for the message model. Storage is created and initialised automatically
    base = init_storage(metadata, engine, Base)

    # Get the table where the current entities are stored
    Entity = getattr(base.classes, metadata.entity)
    entity_id_field = metadata.id_column

    # Get all current non-deleted ids for this source
    session = Session(engine)
    current_ids = session.query(Entity._source_id).filter_by(_source=metadata.source, _date_deleted=None).all()

    # Read new content into dictionary
    new_entities = {data['_source_id']: data for data in msg["contents"]}

    # find deletes by comparing current ids to new entities:
    deleted_ids = [current._source_id for current in current_ids if current._source_id not in new_entities]

    # create the list of mutations
    mutations = []

    # create delete mutations
    for deleted_id in deleted_ids:
        # Get the database entity that is to be deleted (identified by source + source_id)
        entity = session.query(Entity).filter_by(_source=metadata.source, _source_id=deleted_id).one()
        # Create a delete mutation (source_id, entity, ...
        mutations.append(_create_delete(deleted_id, entity, entity_id_field))

    # create other mutations
    for new_id, new_entity in new_entities.items():
        # Get any existing entity, possibly None
        entity = session.query(Entity).filter_by(_source=metadata.source,
                                                 _source_id=new_id,
                                                 _date_deleted=None).one_or_none()
        # Create the corresponding mutation
        mutations.append(_create_confirm_modify_or_add(entity, new_entity, entity_id_field))

    session.close()

    _print_report(mutations)

    # Return the result
    return {
        "header": metadata.as_header,
        "summary": None,  # No log, metrics and qa indicators for now
        "contents": mutations,
    }


def _create_delete(deleted_id, entity, entity_id_field):
    """
    Create an event that specifies the deletion of an entity

    The get_modification method of the GOB.Deleted class is used to get the modification contents.
    For a delete this is simply the id, e.g. entity 123 is deleted

    :param deleted_id:
    :param entity:
    :param entity_id_field:
    :return:
    """
    entity_id = getattr(entity, entity_id_field)
    return GOB.DELETED.get_modification(deleted_id, entity_id_field, entity_id)


def _create_confirm_modify_or_add(entity, new_entity, entity_id_field):
    """
    Create an ADD, MODIFIED or CONFIRMED event

    :param entity:
    :param new_entity:
    :param entity_id_field:
    :return:
    """
    source_id = new_entity['_source_id']
    entity_id = new_entity[entity_id_field]

    if entity is None:
        # No current entity exists
        # Create ADD event, include the complete new entity
        return GOB.ADD.get_modification(source_id, entity_id_field, entity_id, contents=new_entity)
    else:
        # A current entity exists, check if there are any changed attributes
        mutations = _calculate_mutations(entity, new_entity)

        if len(mutations) == 0:
            # No changed attributes => CONFIRM current entity
            return GOB.CONFIRMED.get_modification(source_id, entity_id_field, entity_id)
        else:
            # Create MODIFIED event, include the modified fields (mutations)
            return GOB.MODIFIED.get_modification(source_id, entity_id_field, entity_id, mutations=mutations)


# def _is_equal(entity, new_entity):
#     """
#     Check if an entity is equal to another entity
#
#     Skip all derived and meta data by filtering on not starting with "_"
#
#     :param entity:
#     :param new_entity:
#     :return:
#     """
#     attributes_to_check = [key for key, v in new_entity.items() if not key.startswith('_')]
#
#     for attribute in attributes_to_check:
#         if not _is_attr_equal(new_entity[attribute], getattr(entity, attribute)):
#             return False
#
#     return True


def _is_attr_equal(new_value, stored_value):
    """
    Determine if a new value is equal to a already stored value

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
        if new_value != str(stored_value):
            print(new_value, stored_value, str(stored_value))
        return new_value == str(stored_value)
    else:
        return new_value == stored_value


def _calculate_mutations(entity, new_entity):
    """
    Compare the current entity (entity) with the new entity and return a list of modified attributes
    Each attribute modification is specified by the name of the attribute (key), the current value
    and the new value

    If both entities compare equal an empty list is returned

    :param entity:
    :param new_entity:
    :return:
    """
    attr_modifications = []
    attributes_to_check = [key for key, v in new_entity.items() if not key.startswith('_')]

    for attribute in attributes_to_check:
        if not _is_attr_equal(new_entity[attribute], getattr(entity, attribute)):
            attr_modifications.append({
                "key": attribute,
                "new_value": new_entity[attribute],
                "old_value": getattr(entity, attribute)
            })

    return attr_modifications


def _print_report(mutations):
    """
    Print a simple report telling how many of each events has been processed

    :param mutations:
    :return:
    """
    print(f"Aantal mutaties: {len(mutations)}")
    for event in GOB_EVENTS:
        events = [mutation for mutation in mutations if mutation['action'] == event.name]
        if len(events) > 0:
            print(f"- {event.name}: {len(events)}")
