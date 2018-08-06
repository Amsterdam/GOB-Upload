"""Update the current data

Process events and apply the event on the current state of the entity
"""
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine, Boolean
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from gobuploadservice.config import GOB_DB

from gobuploadservice.gob_config import GOBHeader, GOB_TIMESTAMPS

"""SQLAlchemy engine that encapsulates the database"""
engine = create_engine(URL(**GOB_DB))

# prepare base for autoreflecting existing tables
Base = automap_base()
Base.prepare(engine, reflect=True)
Base.metadata.reflect(bind=engine)


def full_update(msg):
    """Apply the actions on the current dataset

    :param msg: the result of the application of the actions
    :return:
    """
    # Interpret the message header
    metadata = GOBHeader(msg)

    # Reflect on the database to get an Object-mapping to the entity
    Entity = getattr(Base.classes, metadata.entity)

    session = Session(engine)
    mutations = msg["contents"]
    for mutation in mutations:
        data = mutation['contents']
        action = mutation["action"]

        # read and remove relevant id's (requires `compare` to put them in)
        entity_id = data.pop(metadata.id_column)
        source_id = data.pop(metadata.source_id_column)

        entity = session.query(Entity).filter_by(_source=metadata.source,
                                                 _source_id=source_id).one_or_none()

        if action == "ADD" and entity is None:
            # create a new Entity
            entity = Entity(_source_id=source_id, _source=metadata.source)
            setattr(entity, metadata.id_column, entity_id)
            session.add(entity)

        if action == "ADD" and entity._date_deleted is not None:
            entity._date_deleted = None

        assert entity._date_deleted is None

        # add timestamp
        timestamp_field = GOB_TIMESTAMPS[action]
        data[timestamp_field] = metadata.timestamp

        # hydrate the object with data:
        _hydrate(entity, Entity, data, action)

    # Todo: think about transactional integrity, here the db update can be succesfull, while the report back can fail,
    #       triggering a requeue.

    session.commit()
    session.close()

    _print_report(mutations)

    # Return the result message
    return {
        "header": metadata.as_header,
        "summary": None,  # No log, metrics and qa indicators for now
        "contents": None,
    }


def _hydrate(entity, model, data, action):
    """Sets the attributes in data on the entity (expands `data['modifications'] first)

    :param entity: the instance to be modified
    :param data: a collection of key/values to be interpretated
    :param action: one of ADD, MODIFIED, DELETED, CONFIRMED
    :return:
    """

    if action == "MODIFIED":
        # remove modifications from data, expand them into key, value pairs to set on entity
        #   while checking the old value is correct
        set_attributes = _extract_modified_attributes(entity, data.pop('modifications'))
        data = {**data, **set_attributes}

    for key, value in data.items():
        # todo make this more generic (this should not be read from sqlalchemy model, but from GOB-model
        if isinstance(getattr(model, key).prop.columns[0].type, Boolean):
            setattr(entity, key, bool(value))
        else:
            setattr(entity, key, value)


def _extract_modified_attributes(entity, modifications):
    """extracts attributes to modify, and checks if old values are indeed present on entity

    :param entity: the instance to be modified
    :param modifications: a collection of modifications of attributes to be interpretated
    :return: a dict with extracted and verified modifications
    """
    modified_attributes = {}

    for modification in modifications:
        current_val = getattr(entity, modification['key'])
        expected_val = modification['old_value']
        if current_val != expected_val:
            msg = f"Trying to modify data that is not in sync: entity id {entity._id}, " \
                  f"attribute {modification['key']} had value '{current_val}', but expected was '{expected_val}'"
            raise RuntimeError(msg)
        else:
            modified_attributes[modification['key']] = modification['new_value']
    return modified_attributes


def _print_report(mutations):
    # Provide for a simple report
    print(f"Aantal mutaties: {len(mutations)}")
    for action in ["ADD", "MODIFIED", "CONFIRMED", "DELETED"]:
        actions = [mutation for mutation in mutations if mutation['action'] == action]
        if len(actions) > 0:
            print(f"- {action}: {len(actions)}")
