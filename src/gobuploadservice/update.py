"""Update the current data

Process events and apply the event on the current state of the entity
"""
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy.ext.declarative import declarative_base

from gobuploadservice.config import GOB_DB

"""SQLAlchemy engine that encapsulates the database"""
engine = create_engine(URL(**GOB_DB))

Base = declarative_base()
Base.metadata.reflect(bind=engine)


def full_update(msg):
    """Apply the actions on the current dataset

    :param msg: the result of the application of the actions
    :return:
    """
    # Interpret the message header
    header = msg["header"]
    entity = header["entity"]

    # Get the table for the dataset
    entities = Table(entity, Base.metadata)

    mutations = msg["contents"]
    for mutation in mutations:
        # Apply each event
        stmt = execute_mutation(entities, mutation, header)
        engine.execute(stmt)

    print_report(mutations)

    # Return the result message
    return {
        "header": header,
        "summary": None,  # No log, metrics and qa indicators for now
        "contents": None,
    }


def execute_mutation(entities, mutation, header):
    entity_id = header["entity_id"]
    source = header["source"]
    source_id = header["source_id"]
    timestamp = header["timestamp"]
    contents = mutation["contents"]

    where_clause = _get_where_clause(source)

    if mutation["action"] == "ADD":
        return _calculate_add(contents, entities, entity_id, source, source_id, timestamp)
    elif mutation["action"] == "MODIFIED":
        return _calculate_modified(contents, entities, timestamp, where_clause)
    elif mutation["action"] == "DELETED":
        return _calculate_delete(contents, entities, timestamp, where_clause)
    elif mutation["action"] == "CONFIRMED":
        return _calculate_confirm(contents, entities, timestamp, where_clause)
    else:
        raise NotImplementedError


def _get_where_clause(source):
    # The event needs to be applied to the entity with the same source and source_id
    def where_clause(id):
        return f"_source = '{source}' and _source_id = '{id}'"

    return where_clause


def _calculate_confirm(contents, entities, timestamp, where_clause):
    id = contents
    stmt = entities.update().where(where_clause(id)).values({
        "_date_confirmed": timestamp,
        "_date_deleted": None
    })
    return stmt


def _calculate_delete(contents, entities, timestamp, where_clause):
    id = contents
    stmt = entities.update().where(where_clause(id)).values({"_date_deleted": timestamp})
    return stmt


def _calculate_modified(contents, entities, timestamp, where_clause):
    # Modify an existing enity
    id = contents["id"]
    # Collect the modified values
    values = {}
    for modification in contents["modifications"]:
        values[modification["key"]] = modification["new_value"]
    # Register the modification timestamp
    values["_date_modified"] = timestamp
    # A modified entity can not be a deleted entity
    values["_date_deleted"] = None
    stmt = entities.update().where(where_clause(id)).values(values)
    return stmt


def _calculate_add(contents, entities, entity_id, source, source_id, timestamp):
    # Creation of a new entity
    # The universal id is equal to the id of the entity, eg _id and stadsdeelid have equal values
    contents["_id"] = contents[entity_id]
    # Record the date at which the entity has first been read (= importclient timestamp !)
    contents["_date_created"] = timestamp
    # Record source and source_id
    contents["_source"] = source
    contents["_source_id"] = contents[source_id]
    stmt = entities.insert().values(contents)
    return stmt


def print_report(mutations):
    # Provide for a simple report
    print(f"Aantal mutaties: {len(mutations)}")
    for action in ["ADD", "MODIFIED", "CONFIRMED", "DELETED"]:
        actions = [mutation for mutation in mutations if mutation['action'] == action]
        if len(actions) > 0:
            print(f"- {action}: {len(actions)}")
