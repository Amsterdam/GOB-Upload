"""Update the current data

Process events and apply the event on the current state of the entity
"""
from sqlalchemy import create_engine, Boolean
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import Session

from gobcore.events import GOB_EVENTS, get_event, GOB
from gobcore.message_broker.message import GOBHeader
from gobcore.models.event import create_event

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
    metadata = GOBHeader(msg)
    base = init_storage(metadata, engine, Base)

    # Reflect on the database to get an Object-mapping to the entity
    Entity = getattr(base.classes, metadata.entity)
    Event = base.classes.event

    session = Session(engine)
    modifications = msg["contents"]
    for modification in modifications:
        data = modification['contents']
        event = get_event(modification["action"])

        session.add(create_event(Event, event, data, metadata))

        # read and remove relevant id's (requires `compare` to put them in)
        entity_id = data.pop(metadata.id_column)
        source_id = data.pop(metadata.source_id_column)

        entity = session.query(Entity).filter_by(_source=metadata.source,
                                                 _source_id=source_id).one_or_none()

        if event == GOB.ADD and entity is None:
            # create a new Entity
            entity = Entity(_source_id=source_id, _source=metadata.source)
            setattr(entity, metadata.id_column, entity_id)
            session.add(entity)

        if event == GOB.ADD and entity._date_deleted is not None:
            entity._date_deleted = None

        assert entity._date_deleted is None

        # add timestamp
        data[event.timestamp_field] = metadata.timestamp

        # hydrate the object with data:
        _hydrate(entity, Entity, data, event)

    # Todo: think about transactional integrity, here the db update can be succesfull, while the report back can fail,
    #       triggering a requeue.

    session.commit()
    session.close()

    _print_report(modifications)

    # Return the result message
    return {
        "header": metadata.as_header,
        "summary": None,  # No log, metrics and qa indicators for now
        "contents": None,
    }


def _hydrate(entity, model, data, event):
    """Sets the attributes in data on the entity (expands `data['mutations'] first)

    :param entity: the instance to be modified
    :param data: a collection of key/values to be interpretated
    :param event: one of ADD, MODIFIED, DELETED, CONFIRMED
    :return:
    """

    if event == GOB.MODIFIED:
        # remove mutations from data, expand them into key, value pairs to set on entity
        #   while checking the old value is correct
        set_attributes = _extract_modified_attributes(entity, data.pop('mutations'))
        data = {**data, **set_attributes}

    for key, value in data.items():
        # todo make this more generic (this should not be read from sqlalchemy model, but from GOB-model
        if isinstance(getattr(model, key).prop.columns[0].type, Boolean):
            setattr(entity, key, bool(value))
        else:
            setattr(entity, key, value)


def _extract_modified_attributes(entity, mutations):
    """extracts attributes to modify, and checks if old values are indeed present on entity

    :param entity: the instance to be modified
    :param mutations: a collection of mutations of attributes to be interpretated
    :return: a dict with extracted and verified mutations
    """
    modified_attributes = {}

    for mutation in mutations:
        current_val = getattr(entity, mutation['key'])
        expected_val = mutation['old_value']
        if current_val != expected_val:
            msg = f"Trying to modify data that is not in sync: entity id {entity._id}, " \
                  f"attribute {mutation['key']} had value '{current_val}', but expected was '{expected_val}'"
            raise RuntimeError(msg)
        else:
            modified_attributes[mutation['key']] = mutation['new_value']
    return modified_attributes


def _print_report(mutations):
    # Provide for a simple report
    print(f"Aantal mutaties: {len(mutations)}")
    for event in GOB_EVENTS:
        events = [mutation for mutation in mutations if mutation['action'] == event.name]
        if len(events) > 0:
            print(f"- {event.name}: {len(events)}")
