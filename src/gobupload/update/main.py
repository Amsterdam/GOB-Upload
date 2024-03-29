"""Update the current data

Process events and apply the event on the current state of the entity
"""
from typing import Iterator

from more_itertools import ichunked

from gobcore.events.import_message import ImportMessage
from gobcore.logging.logger import logger
from gobcore.utils import ProgressTicker
from gobupload.storage.handler import GOBStorageHandler
from gobupload.update.event_collector import EventCollector
from gobupload.update.update_statistics import UpdateStatistics
from gobupload.utils import get_event_ids, is_corrupted


def _store_events(
        storage: GOBStorageHandler,
        last_events: dict[str, int],
        events: Iterator,
        stats: UpdateStatistics
):
    """
    Store events in GOB.

    Only valid events are stored, other events are skipped (with an associated warning)
    The events are added in bulk in the database

    :param storage: GOB (events + entities)
    :param last_events:
    :param events: the events to process
    :param stats: update statitics for this action
    :return:
    """
    logger.info("Store events")
    chunksize = 10_000

    with (
        ProgressTicker("Store events", chunksize) as progress,
        storage.get_session() as session,
        EventCollector(storage, last_events) as event_collector,

        # explicitely start transaction context on bind
        # storage.add_events operates on the bind and not the session
        # nothing is committed otherwise
        session.bind.begin()
    ):
        for chunk in ichunked(events, chunksize):
            for event in chunk:
                progress.tick()

                if event_collector.is_valid(event):
                    event_collector.collect(event)
                    stats.store_event(event)
                else:
                    logger.warning(f"Invalid event: {event}")
                    stats.skip_event(event)

            event_collector.store_events()


def _process_events(storage, events, stats):
    """Store and apply events

    :param storage: GOB (events + entities)
    :param event: the event to process
    :param stats: update statitics for this action
    :return:
    """
    # Get the max eventid of the entities and the last eventid of the events
    entity_max_eventid, last_eventid = get_event_ids(storage)
    logger.info(f"Events are at {last_eventid or 0:,}, model is at {entity_max_eventid or 0:,}")

    # Get all source_id - last_event combinations to check for validity and existence
    with storage.get_session():
        last_events = storage.get_last_events()  # { source_id: last_event, ... }

    if is_corrupted(entity_max_eventid, last_eventid):
        logger.error("Model is inconsistent! data is more recent than events")
    elif entity_max_eventid == last_eventid:
        logger.info("Model is up to date")
        # Add new events
        return _store_events(storage, last_events, events, stats)
    else:
        logger.warning("Model is out of date, Further processing has stopped")


def full_update(msg):
    """Store the events for the current dataset

    :param msg: the result of the application of the events
    :return: Result message
    """
    logger.info(f"Update to GOB Database {GOBStorageHandler.user_name} started")

    # Interpret the message header
    message = ImportMessage(msg)
    metadata = message.metadata

    storage = GOBStorageHandler(metadata)
    model = f"{metadata.source} {metadata.catalogue} {metadata.entity}"
    logger.info(f"Store events {model}")

    # Get events from message
    events = msg["contents"]

    # Gather statistics of update process
    stats = UpdateStatistics()

    _process_events(storage, events, stats)

    # Build result message
    results = stats.results()

    stats.log()
    logger.info(f"Store events {model} completed", {'data': results})

    results.update(logger.get_summary())

    # Return the result message, with no log, no contents but pass-through any confirms
    message = {
        "header": msg["header"],
        "summary": results,
        "contents": None,
        "confirms": msg.get('confirms')
    }
    return message
