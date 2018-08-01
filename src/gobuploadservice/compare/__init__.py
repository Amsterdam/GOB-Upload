"""Compare new data with the existing data

Derive Add, Change, Delete and Confirm actions by comparing a full set of new data against the full set of current data

"""
import datetime
from decimal import Decimal

from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from gobuploadservice.config import GOB_DB

"""SQLAlchemy engine that encapsulates the database"""
engine = create_engine(URL(**GOB_DB))

Base = declarative_base()
Base.metadata.reflect(bind=engine)

session = sessionmaker()
session.configure(bind=engine)
gob_session = session()


def is_equal(new_value, stored_value):
    """Determine if a new value is equal to a already stored value

    Values may be stored in a different format than the new data. Convert the data where required

    :param new_value: The new value
    :param stored_value: The currently stored value
    :return: True if both values compare equal
    """
    if isinstance(stored_value, datetime.date):
        return new_value == stored_value.strftime("%Y-%m-%d")
    elif isinstance(stored_value, Decimal):
        return new_value == str(stored_value)
    else:
        return new_value == stored_value


def compare(msg):
    """Compare new data in msg (contents) with the current data

    :param msg: The new data, including header and summary
    :return: result message
    """

    # Parse the message header
    header = msg["header"]
    entity = header["entity"]
    source = header["source"]
    source_id = header["source_id"]
    version = header["version"]

    # Do any migrations if the data is behind in version
    if version != "0.1":
        # No migrations defined yet...
        raise ValueError("Unexpected version, please write a generic migration here of migrate the import")

    # Get the table where the current entities are stored
    entities = Table(entity, Base.metadata)

    # Get all current data for the source
    all = gob_session.query(entities).filter(text(f"_source='{source}'")).all()

    # Convert the data to a dictionary for fast lookup on source_id
    cur_values = {getattr(data, source_id): data for data in all}
    new_values = {data[source_id]: data for data in msg["contents"]}

    # Get all current and new source ids
    cur_ids = [value for value in cur_values.keys()]
    new_ids = [value for value in new_values.keys()]

    # Derive the mutations
    mutations = []

    # Loop over all source_ids (both new and existing)
    for id in set(cur_ids + new_ids):
        # Get the new value from the dictionary
        new_value = new_values.get(id)

        # Get the current value from the dictionary and transform it into an object
        # Skip all derived and meta data by filtering on not starting with "_"
        cur_value = cur_values.get(id)
        cur_value = {attr: getattr(cur_value, attr) for attr in dir(cur_value) if not attr.startswith('_')}

        # Determine the appropriate action
        if not cur_value:
            # Entity does not yet exist, create it
            mutation = {
                "action": "ADD",
                "contents": new_value
            }
        elif new_value is None:
            # Entity no longer exists in source
            if cur_value["_date_deleted"] is None:
                # Has the deletion already been processed?
                mutation = {
                    "action": "DELETED",
                    "contents": id
                }
            else:
                # Do not delete twice
                continue
        else:
            # Modified or Confirmed entry
            modifications = []
            # Compare each value
            for attr, value in new_value.items():
                if not is_equal(new_value.get(attr), cur_value.get(attr)):
                    # Create a modification request for the changed value
                    modifications.append({
                        "key": attr,
                        "new_value": new_value.get(attr),
                        "old_value": cur_value.get(attr)
                    })
            if len(modifications) == 0:
                # Every value compares equal, entity is confirmed
                mutation = {
                    "action": "CONFIRMED",
                    "contents": id
                }
            else:
                # Some values have changed, entity is modified
                mutation = {
                    "action": "MODIFIED",
                    "contents": {
                        "id": id,
                        "modifications": modifications
                    }
                }

        mutations.append(mutation)

    # Print a simple report
    print(f"Aantal mutaties: {len(mutations)}")
    for action in ["ADD", "MODIFIED", "CONFIRMED", "DELETED"]:
        actions = [mutation for mutation in mutations if mutation['action'] == action]
        if len(actions) > 0:
            print(f"- {action}: {len(actions)}")

    # Return the result
    return {
        "header": header,
        "summary": None,  # No log, metrics and qa indicators for now
        "contents": mutations,
    }
