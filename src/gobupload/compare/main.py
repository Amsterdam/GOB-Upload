"""Compare new data with the existing data

Derive Add, Change, Delete and Confirm events by comparing a full set of new data against the full set of current data

Todo: Event, action and mutation are used for the same subject. Use one name to improve maintainability.

"""

from gobcore.events import get_event_for, GOB
from gobcore.events.import_message import ImportMessage
from gobcore.model import GOBModel
from gobcore.typesystem import get_modifications
from gobcore.logging.logger import logger
from gobcore.message_broker.offline_contents import ContentsWriter
from gobcore.utils import ProgressTicker

from gobupload.storage.handler import GOBStorageHandler
from gobupload.compare.enrich import Enricher
from gobupload.compare.populate import Populator
from gobupload.compare.entity_collector import EntityCollector
from gobupload.compare.event_collector import EventCollector
from gobupload.compare.compare_statistics import CompareStatistics
from gobupload.config import FULL_UPLOAD


def compare(msg):
    """Compare new data in msg (contents) with the current data

    :param msg: The new data, including header and summary
    :return: result message
    """
    logger.configure(msg, "COMPARE")
    header = msg.get('header', {})
    mode = header.get('mode', FULL_UPLOAD)
    logger.info(f"Compare (mode = {mode}) to GOB Database {GOBStorageHandler.user_name} started")

    # Parse the message header
    message = ImportMessage(msg)
    metadata = message.metadata

    # Get the model for the collection to be compared
    gob_model = GOBModel()
    entity_model = gob_model.get_collection(metadata.catalogue, metadata.entity)

    # Initialize a storage handler for the collection
    storage = GOBStorageHandler(metadata)
    model = f"{metadata.source} {metadata.catalogue} {metadata.entity}"
    logger.info(f"Compare {model}")

    stats = CompareStatistics()

    tmp_table_name = None
    with storage.get_session():
        with ProgressTicker("Collect compare events", 10000) as progress:
            # Check any dependencies
            if not meets_dependencies(storage, msg):
                return {
                    "header": msg["header"],
                    "summary": {
                        'warnings': logger.get_warnings(),
                        'errors': logger.get_errors()
                    },
                    "contents": None
                }

            enricher = Enricher(storage, msg)
            populator = Populator(entity_model, msg)

            # If there are no records in the database all data are ADD events
            initial_add = not storage.has_any_entity()
            if initial_add:
                logger.info("Initial load of new collection detected")
                # Write ADD events directly, without using a temporary table
                contents_writer = ContentsWriter()
                contents_writer.open()
                # Pass a None confirms_writer because only ADD events are written
                collector = EventCollector(contents_writer, confirms_writer=None)
                collect = collector.collect_initial_add
            else:
                # Collect entities in a temporary table
                collector = EntityCollector(storage)
                collect = collector.collect
                tmp_table_name = collector.tmp_table_name

            for entity in msg["contents"]:
                progress.tick()
                stats.collect(entity)
                enricher.enrich(entity)
                populator.populate(entity)
                collect(entity)

            collector.close()

    if initial_add:
        filename = contents_writer.filename
        confirms = None
        contents_writer.close()
    else:
        # Compare entities from temporary table
        with storage.get_session():
            diff = storage.compare_temporary_data(tmp_table_name, mode)
            filename, confirms = _process_compare_results(storage, entity_model, diff, stats)

    # Build result message
    results = stats.results()

    logger.info(f"Compare {model} completed", {'data': results})

    results.update({
        'warnings': logger.get_warnings(),
        'errors': logger.get_errors()
    })

    message = {
        "header": msg["header"],
        "summary": results,
        "contents_ref": filename,
        "confirms": confirms
    }

    return message


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


def _process_compare_results(storage, model, results, stats):
    """Process the results of the in database compare

    Creates the ADD, DELETE and CONFIRM records and returns them with the remaining records

    :param results: the result rows from the database comparison
    :param data_by_source_id: a mapping of import data by source_id
    :return: list of events, list of remaining records
    """
    # Take two files: one for confirms and one for other events
    with ProgressTicker("Process compare result", 10000) as progress, \
            ContentsWriter() as contents_writer, \
            ContentsWriter() as confirms_writer, \
            EventCollector(contents_writer, confirms_writer) as event_collector:

        filename = contents_writer.filename
        confirms = confirms_writer.filename

        for row in results:
            progress.tick()
            # Get the data for this record and create the event
            entity = row["_original_value"]

            # _source_id is the source id of the new entity
            # _entity_source_id is the source id of the current entity

            stats.compare(row)

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
                modifications = get_modifications(current_entity, entity, model['all_fields'])
                event = get_event_for(current_entity, entity, modifications)
            elif row['type'] == 'DELETE':
                source_id = row['_entity_source_id']
                data = {
                    '_last_event': row['_last_event']
                }
                event = GOB.DELETE.create_event(source_id, source_id, data)
            else:
                continue

            event_collector.collect(event)

    return filename, confirms
