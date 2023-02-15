"""Compare new data with the existing data.

Derive Add, Change, Delete and Confirm events by comparing a full set of new data against the full set of current data.

Todo: Event, action and mutation are used for the same subject. Use one name to improve maintainability.
"""
from typing import Iterator, Callable

from gobcore.enum import ImportMode
from gobcore.events import get_event_for, GOB
from gobcore.events.import_message import ImportMessage
from gobcore.typesystem import get_modifications
from gobcore.logging.logger import logger
from gobcore.message_broker.offline_contents import ContentsWriter
from gobcore.utils import ProgressTicker

from gobupload import gob_model
from gobupload.config import FULL_UPLOAD
from gobupload.storage.handler import GOBStorageHandler
from gobupload.compare.enrich import Enricher
from gobupload.compare.populate import Populator
from gobupload.compare.entity_collector import EntityCollector
from gobupload.compare.event_collector import EventCollector
from gobupload.compare.compare_statistics import CompareStatistics


def _collect_entities(
        entities: Iterator[dict],
        collect: Callable[[dict], None],
        enricher: Enricher,
        populator: Populator,
        stats: CompareStatistics
):
    with ProgressTicker("Collect compare events", 10_000) as progress:
        for entity in entities:
            progress.tick()
            stats.collect(entity)
            enricher.enrich(entity)
            populator.populate(entity)
            collect(entity)


def compare(msg):
    """Compare new data in msg (contents) with the current data.

    :param msg: The new data, including header and summary
    :return: result message
    """
    header = msg.get('header', {})
    mode = ImportMode(header.get('mode', FULL_UPLOAD))
    logger.info(f"Compare (mode = {mode.name}) to GOB Database {GOBStorageHandler.user_name} started")

    # Parse the message header
    message = ImportMessage(msg)
    metadata = message.metadata

    # Get the collection to be compared
    entity_model = gob_model[metadata.catalogue]['collections'][metadata.entity]
    version = entity_model['version']

    # Initialize a storage handler for the collection
    storage = GOBStorageHandler(metadata)

    model = f"{metadata.source} {metadata.catalogue} {metadata.entity}"
    logger.info(f"Compare {model}")

    stats = CompareStatistics()

    with storage.get_session(invalidate=True, yield_per=10_000):
        # Check any dependencies
        if not meets_dependencies(storage, msg):
            return {
                "header": msg["header"],
                "summary": logger.get_summary(),
                "contents": None
            }

        enricher = Enricher(storage, msg)
        populator = Populator(entity_model, msg)

        if storage.has_any_entity():
            # Collect entities in a temporary table
            with EntityCollector(storage) as collector:
                _collect_entities(msg["contents"], collector.collect, enricher, populator, stats)

            diff = storage.compare_temporary_data(mode)
            filename, confirms = _process_compare_results(storage, entity_model, diff, stats)

        else:
            # If there are no records in the database all data are ADD events
            logger.info("Initial load of new collection detected")

            with (
                ContentsWriter() as writer,
                EventCollector(contents_writer=writer, confirms_writer=None, version=version) as collector
            ):
                _collect_entities(msg["contents"], collector.collect_initial_add, enricher, populator, stats)

            filename, confirms = writer.filename, None

    # Build result message
    results = stats.results()

    logger.info(f"Compare {model} completed", {'data': results})

    results.update(logger.get_summary())

    message = {
        "header": msg["header"],
        "summary": results,
        "contents_ref": filename,
        "confirms": confirms
    }

    return message


def meets_dependencies(storage, msg):
    """Check if all dependencies are met.

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
    """Process the results of the in database compare.

    Creates the ADD, DELETE and CONFIRM records and returns them with the remaining records.

    :param results: the result rows from the database comparison
    :return: list of events, list of remaining records
    """
    version = model['version']
    # Take two files: one for confirms and one for other events
    with ProgressTicker("Process compare result", 10000) as progress, \
            ContentsWriter() as contents_writer, \
            ContentsWriter() as confirms_writer, \
            EventCollector(contents_writer, confirms_writer, version) as event_collector:

        filename = contents_writer.filename
        confirms = confirms_writer.filename

        for row in results:
            progress.tick()
            # Get the data for this record and create the event
            entity = row["_original_value"]

            stats.compare(row)

            if row['type'] == 'ADD':
                entity["_last_event"] = row['_last_event']
                event = GOB.ADD.create_event(row['_tid'], entity, version)
            elif row['type'] == 'CONFIRM':
                data = {
                    '_last_event': row['_last_event']
                }
                event = GOB.CONFIRM.create_event(row['_tid'], data, version)
            elif row['type'] == 'MODIFY':
                current_entity = storage.get_current_entity(entity)
                modifications = get_modifications(current_entity, entity, model['all_fields'])
                event = get_event_for(current_entity, entity, modifications, version)
            elif row['type'] == 'DELETE':
                data = {
                    '_last_event': row['_last_event']
                }
                event = GOB.DELETE.create_event(row['_entity_tid'], data, version)
            else:
                continue

            event_collector.collect(event)

    return filename, confirms
