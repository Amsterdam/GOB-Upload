"""Compare new data with the existing data

Derive Add, Change, Delete and Confirm events by comparing a full set of new data against the full set of current data

Todo: Event, action and mutation are used for the same subject. Use one name to improve maintainability.

"""

from gobcore.events import get_event_for, GOB
from gobcore.events.import_message import ImportMessage
from gobcore.model import GOBModel
from gobcore.typesystem import get_modifications
from gobcore.logging.logger import logger

from gobupload import get_report
from gobupload.storage.handler import GOBStorageHandler
from gobupload.compare.enrich import Enricher
from gobupload.compare.populate import Populator
from gobupload.compare.entity_collector import EntityCollector
from gobupload.compare.event_collector import EventCollector


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

        enricher = Enricher(storage, msg)
        populator = Populator(entity_model, msg)

        with EntityCollector(storage) as entity_collector:
            for entity in msg["contents"]:
                enricher.enrich(entity)
                populator.populate(entity)
                entity_collector.collect(entity)

        diff = storage.compare_temporary_data()
        events = _process_compare_results(storage, entity_model, diff)

    results = get_report(msg["contents"], events)
    logger.info(f"Message processed", kwargs={'data': results})

    msg_contents = {
        "events": events
    }

    # Return the result without log.
    return ImportMessage.create_import_message(msg["header"], None, msg_contents)


def meets_dependencies(storage, msg):
    """Check if all dependencies are met

    :param storage: Storage handler
    :param msg: Incoming message
    :return: True if all dependencies are met, else False
    """
    depends_on = msg["header"].get("depends_on", {})
    for key, value in depends_on.items():
        if key[0] == '_':
            # Temporary fix for compatibility with current import definitions
            # https://github.com/Amsterdam/GOB-Upload/issues/181
            key = key[1:]
        # Check every dependency
        if not storage.has_any_event({key: value}):
            logger.error(f"Compare failed; dependency {value} not fulfilled.")
            return False
    return True


def _process_compare_results(storage, model, results):
    """Process the results of the in database compare

    Creates the ADD, DELETE and CONFIRM records and returns them with the remaining records

    :param results: the result rows from the database comparison
    :param data_by_source_id: a mapping of import data by source_id
    :return: list of events, list of remaining records
    """
    result = []
    with EventCollector() as events:

        for row in results:
            # Get the data for this record and create the event
            entity = row["_original_value"]

            if row['type'] == 'ADD':
                source_id = row['_source_id']
                entity["_last_event"] = row['_last_event']
                event = GOB.ADD.create_event(source_id, source_id, entity)
            elif row['type'] == 'CONFIRM':
                source_id = row['_source_id']
                data = {
                    '_last_event': row['_last_event']
                }
                event = GOB.CONFIRM.create_event(source_id, source_id, data)
            elif row['type'] == 'MODIFY':
                current_entity = storage.get_current_entity(entity)
                modifications = get_modifications(current_entity, entity, model['fields'])
                event = get_event_for(current_entity, entity, modifications)
            elif row['type'] == 'DELETE':
                source_id = row['_entity_source_id']
                data = {
                    '_last_event': row['_last_event']
                }
                event = GOB.DELETE.create_event(source_id, source_id, data)

            events.add(event)

        result = events.events

    return result
