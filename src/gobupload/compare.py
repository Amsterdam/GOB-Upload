"""Compare new data with the existing data

Derive Add, Change, Delete and Confirm events by comparing a full set of new data against the full set of current data

Todo: Event, action and mutation are used for the same subject. Use one name to improve maintainability.

"""
import hashlib
import json

from gobcore.events import get_event_for, GOB
from gobcore.events.import_message import ImportMessage
from gobcore.model import GOBModel
from gobcore.typesystem import get_modifications
from gobcore.typesystem.json import GobTypeJSONEncoder
from gobcore.logging.logger import logger

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

        # Add required fields (_hash, _version, _id, ...) to each record
        populate(msg, entity_model)

        # Perform a compare using a hash to detect differences between the current state and the new data
        events, remaining_records = _shallow_compare(storage, entity_model, msg)

        # Convert the remaining msg contents in events
        modify_events = _process_new_data(storage, entity_model, remaining_records)

        # Add the created modify events to the result
        events.extend(modify_events)

    results = get_report(msg["contents"], events)
    logger.info(f"Message processed", kwargs={'data': results})

    msg_contents = {
        "events": events
    }

    # Return the result without log.
    return ImportMessage.create_import_message(msg["header"], None, msg_contents)


class Populator:

    def __init__(self, msg, entity_model):
        self.id_column = entity_model["entity_id"]
        self.version = entity_model["version"]
        self.application = msg['header']['application']

    def populate(self, entity):
        entity["_id"] = entity[self.id_column]
        entity["_version"] = self.version
        entity['_hash'] = hashlib.md5((json.dumps(entity, sort_keys=True, cls=GobTypeJSONEncoder) +
                                       self.application).encode('utf-8')
                                      ).hexdigest()


def populate(msg, entity_model):
    """Add an md5 hash of the record to the record for comparison

    :param msg: Incoming message
    :return:
    """
    populator = Populator(msg, entity_model)
    for entity in msg["contents"]:
        populator.populate(entity)


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


def _shallow_compare(storage, model, msg):
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

    events, remaining_records = _process_compare_results(results, data_by_source_id)

    # Add deletions which could not have been found by comparing in database
    events.extend(_process_deletions(storage, model, data_by_source_id.keys()))

    return events, remaining_records


def _process_compare_results(results, data_by_source_id):
    """Process the results of the in database compare

    Creates the ADD, DELETE and CONFIRM records and returns them with the remaining records

    :param results: the result rows from the database comparison
    :param data_by_source_id: a mapping of import data by source_id
    :return: list of events, list of remaining records
    """
    events = []
    confirms = []
    remaining_records = []
    for row in results:
        # Get the data for this record and create the event
        data = data_by_source_id.get(row["_source_id"])

        if row['type'] == 'ADD':
            data["_last_event"] = row['_last_event']
            events.append(GOB.ADD.create_event(row['_source_id'], row['_source_id'], data))
        elif row['type'] == 'CONFIRM':
            confirms.append({
                '_source_id': row['_source_id'],
                '_last_event': row['_last_event']
            })
        elif row['type'] == 'MODIFY':
            # Store the data of modify events for further processing and don't create an event
            remaining_records.append(data)

    if confirms:
        events.append(_create_confirm_event(confirms))

    return events, remaining_records


def _create_confirm_event(confirms):
    """Create the CONFIRM or BULKCONFIRM event

    Given a list of confirms, this will return either a single CONFIRM or a
    BULKCONFIRM

    :param confirms: a list of dicts with _source_id, _last_event
    :return: a CONFIRM or BULKCONFIRM event
    """
    # Create a BULKCONFIRM event if multiple confirms are found
    if len(confirms) > 1:
        event = GOB.BULKCONFIRM.create_event(confirms)
    # Create a CONFIRM event if one confirm is found
    else:
        source_id = confirms[0]['_source_id']
        data = {
            '_last_event': confirms[0]['_last_event']
        }
        event = GOB.CONFIRM.create_event(source_id, source_id, data)
    return event


def _process_deletions(storage, model, new_entities):
    """Derive deletions
    By comparing stored data with new data
    :param storage: Storage handler instance for the collection being processed
    :param model: GOB Model for the collection
    :param new_entities: list of source_ids
    :return: list of Delete Events
    """
    # Retrieve current ids for the same collection
    current_ids = storage.get_current_ids()
    # find deletes by comparing current ids to new entities
    # if a current_id is not found in the new_entities it is interpreted as a deletion
    deleted = {current._source_id: None for current in current_ids if current._source_id not in new_entities}
    events = []
    for entity_id, data in deleted.items():
        events.append(_compare_new_data(model, storage, entity_id=entity_id))
    return events


def _process_new_data(storage, model, contents):
    """Convert the remaining data in the message into events

    :param storage: Storage handler instance for the collection being processed
    :param model: GOB Model for the collection
    :param contents: a list of imported records, remaining after first compare
    :return: list of events
    """
    events = []

    for data in contents:
        event = _compare_new_data(model, storage, data)
        # append the event to the events-list to be outputted
        events.append(event)

    return events


def _compare_new_data(model, storage, new_data=None, entity_id=None):
    """Compare new data with any existing data
    :param model: GOB Model for the collection
    :param storage: Storage handler instance for the collection being processed
    :param new_data:
    :param entity_id: entity if of existing data
    :return:
    """
    assert not (new_data is None and entity_id is None), \
        "One of new data or entity ID should be provided"
    if new_data is None:
        # Deletion
        entity = storage.get_entity_or_none(entity_id)
    else:
        # Add, Confirm, Modify. Get current entity to compare with (None if ADD)
        entity = storage.get_current_entity(new_data)
    # calculate modifications, this will be an empty list if either data or entity is empty
    # or if all attributes are equal
    modifications = get_modifications(entity, new_data, model['fields'])
    # construct the event given the entity, data, and metadata
    return get_event_for(entity, new_data, modifications)
