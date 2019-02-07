"""Compare new data with the existing data

Derive Add, Change, Delete and Confirm events by comparing a full set of new data against the full set of current data

Todo: Event, action and mutation are used for the same subject. Use one name to improve maintainability.

"""
import hashlib
import json

from gobcore.events import _get_event, get_event_for, GOB
from gobcore.events.import_message import ImportMessage
from gobcore.model import GOBModel
from gobcore.typesystem import get_modifications
from gobcore.typesystem.json import GobTypeJSONEncoder

from gobupload import logger
from gobupload import get_report
from gobupload.storage.handler import GOBStorageHandler
from gobupload.enrich import enrich


def compare(msg):
    """Compare new data in msg (contents) with the current data

    :param msg: The new data, including header and summary
    :return: result message
    """
    logger.configure(msg, "COMPARE")
    logger.info(f"Compare to GOB Database {GOBStorageHandler.user_name} started")

    # Parse the message header
    message = ImportMessage(msg)
    metadata = message.metadata

    # Get the model for the collection to be compared
    gob_model = GOBModel()
    entity_model = gob_model.get_collection(metadata.catalogue, metadata.entity)

    # Initialize a storage handler for the collection
    storage = GOBStorageHandler(metadata)

    with storage.get_session():
        # Check any dependencies
        if not meets_dependencies(storage, msg):
            return None

        # Enrich data
        enrich(storage, msg)

        # Add a hash to each record for comparision
        add_hash(msg)

        # Perform a compare using a hash to detect differences between the current state and the new data
        events, remaining_records = _shallow_compare(storage, entity_model, msg)

        # Convert the remaining msg contents in events
        modify_events, recompares = _process_new_data(storage, entity_model, remaining_records)

        # Add the created modify events to the result
        events.extend(modify_events)

    results = get_report(msg["contents"], events, recompares)
    logger.info(f"Message processed", kwargs={'data': results})

    msg_contents = {
        "events": events,
        "recompares": recompares
    }

    # Return the result without log.
    return ImportMessage.create_import_message(msg["header"], None, msg_contents)


def add_hash(msg):
    """Add an md5 hash of the record to the record for comparison

    :param msg: Incoming message
    :return:
    """
    for record in msg["contents"]:
        record['_hash'] = hashlib.md5(
            json.dumps(record, sort_keys=True, cls=GobTypeJSONEncoder).encode('utf-8')
        ).hexdigest()


def meets_dependencies(storage, msg):
    """Check if all dependencies are met

    :param storage: Storage handler
    :param msg: Incoming message
    :return: True if all dependencies are met, else False
    """
    depends_on = msg["header"].get("depends_on", {})
    for key, value in depends_on.items():
        # Check every dependency
        if not storage.has_any_entity(key, value):
            logger.error(f"Compare failed; dependency {value} not fulfilled.")
            return False
    return True


def _shallow_compare(storage, model, msg):  # noqa: C901
    """Shallow comparison

    Compare the new data with the current state of the database. The result will be
    ADD, CONFIRM and DELETE events. The remaining data will be MODIFY events and will
    be checked on attribute level in the following functions.

    :param storage: Storage handler instance for the collection being processed
    :param model: GOB Model for the collection
    :param msg:
    :return: list ADD, CONFIRM and DELETE events
             remaining records of the import data for further comparison
    """
    contents = msg["contents"]

    # If there are no records in the database, all data should be ADD events
    if not storage.has_any_entity():
        events = [GOB.ADD.create_event(row['_source_id'], row['_source_id'], row) for row in contents]
        return events, []

    # Create temporary table with new data
    storage.create_temporary_table(contents)

    # Perform a shallow in-database comparison on _hash field
    results = storage.compare_temporary_data()

    data_by_source_id = {row['_source_id']: row for row in contents}

    # Start creating events for ADD, DELETE and CONFIRM
    events = []
    remaining_records = []
    for row in results:
        # Get the data for this record and create the event
        data = data_by_source_id.get(row["_source_id"])

        if row['type'] == 'ADD':
            data["_last_event"] = row['_last_event']
            event = GOB.ADD.create_event(row['_source_id'], row['_source_id'], data)
        elif row['type'] in ['CONFIRM', 'DELETE']:
            # Get the GOB Event type by name
            gob_event = _get_event(row['type'])

            # Confirm and delete events only need the last event and hash
            event = gob_event.create_event(
                row['_source_id'],
                row['_entity_source_id'],
                {
                    '_last_event': row['_last_event'],
                    '_hash': row['_hash'],
                }
            )
        elif row['type'] == 'MODIFY':
            # Store the data of modify events for further processing and don't create an event
            remaining_records.append(data)
            event = None

        if event:
            events.append(event)

    return events, remaining_records


def _process_new_data(storage, model, contents):
    """Convert the data in the message into events and recompares

    Recompares occur when the message contains multiple new volgnummers for the same state
    The volgnummers denote modifications or confirms to the state
    They should be processed in order to have a consistent history for the state

    :param storage: Storage handler instance for the collection being processed
    :param model: GOB Model for the collection
    :param contents: a list of imported records, remaining after first compare
    :return: list of events, list of recompares
    """
    previous_ids = {}
    events = []
    recompares = []

    for data in contents:
        event = _compare_new_data(model, storage, data)
        if event is None:
            # Skip historical states
            continue

        # Check for any multiple new volgnummers, use previous_ids to register volgnummers
        recompare = _get_recompare(model, previous_ids, event, data)
        if recompare is not None:
            recompares.append(recompare)
            continue

        # append the event to the events-list to be outputted
        events.append(event)

    return events, recompares


def _get_recompare(model, previous_ids, event, data):
    """Check for any recompares

    Recompares occur when the message contains multiple new volgnummers for the same state
    The volgnummers denote modifications or confirms to the state
    They should be processed in order to have a consistent history for the state

    If more than 1 sequence number (volgnummer) is in the same set, only the first can be compared
    Later sequence numbers can only be compared if the previous has been applied first

    :param model: GOB Model for the collection
    :param previous_ids: dictionary with previous volgnummers
    :param event: the event for the data
    :param data:
    :return: data if the data should be recompared after application of the other events, else None
    """
    if model.get("has_states", False):
        entity_id = data['_source_id']
        if previous_ids.get(entity_id):
            previous = previous_ids[entity_id]
            assert int(previous["volgnummer"]) < int(data["volgnummer"]), \
                f'Volgnummer should be sequential {entity_id} {previous["volgnummer"]} !< {data["volgnummer"]}'
            previous_ids[entity_id] = data  # Save this data as last previous data
            return data
        elif event['event'] != 'CONFIRM':
            # Prevent multiple changes in one update
            previous_ids[entity_id] = data


def _compare_new_data(model, storage, data):
    """Compare new data with any existing data

    Will only produce MODIFY events as ADD, CONFIRM and DELETE events allready have
    been created by _shallow_compare.

    :param model: GOB Model for the collection
    :param storage: Storage handler instance for the collection being processed
    :param data: The imported new data
    :return:
    """
    assert data, "Data should be provided"

    # Get current entity to compare with
    entity = storage.get_current_entity(data)

    # Skip historic volgnummers
    if model.get("has_states", False):
        # Skip any historic states for collections with state
        new_seqnr = data["volgnummer"]
        old_seqnr = entity.volgnummer
        if int(new_seqnr) < int(old_seqnr):
            return

    # Calculate modifications
    modifications = get_modifications(entity, data, model['fields'])
    # construct the event given the entity, data, and metadata
    return get_event_for(entity, data, modifications)


def recompare(storage, data):
    """Recompare data with stored data

    :param storage: Storage handler instance for the collection being processed
    :param data:
    :return:
    """
    model = storage.get_collection_model()
    return _compare_new_data(model, storage, new_data=data)
