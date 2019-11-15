"""Update the current data

Process events and apply the event on the current state of the entity
"""
from gobcore.events.import_message import ImportMessage
from gobcore.logging.logger import logger
from gobcore.utils import ProgressTicker

from gobupload.storage.handler import GOBStorageHandler
from gobupload.update.update_statistics import UpdateStatistics
from gobupload.update.event_collector import EventCollector
from gobupload.utils import ActiveGarbageCollection, is_corrupted, get_event_ids


def _store_events(storage, last_events, events, stats):
    """Store events in GOB

    Only valid events are stored, other events are skipped (with an associated warning)
    The events are added in bulk in the database

    :param storage: GOB (events + entities)
    :param events: the events to process
    :param stats: update statitics for this action
    :return:
    """
    with ActiveGarbageCollection("Store events"), storage.get_session():
        # Use a session to commit all or rollback on any error
        logger.info(f"Store events")

        with ProgressTicker("Store events", 10000) as progress, \
                EventCollector(storage, last_events) as event_collector:

            for event in events:
                progress.tick()

                if event_collector.collect(event):
                    stats.store_event(event)
                else:
                    stats.skip_event(event)


def _process_events(storage, events, stats):
    """Store and apply events

    :param storage: GOB (events + entities)
    :param event: the event to process
    :param stats: update statitics for this action
    :return:
    """
    # Get the max eventid of the entities and the last eventid of the events
    entity_max_eventid, last_eventid = get_event_ids(storage)

    # Get all source_id - last_event combinations to check for validity and existence
    with storage.get_session():
        last_events = storage.get_last_events()  # { source_id: last_event, ... }

    if is_corrupted(entity_max_eventid, last_eventid):
        logger.error(f"Model is inconsistent! data is more recent than events")
    elif entity_max_eventid == last_eventid:
        logger.info(f"Model is up to date")
        # Add new events
        return _store_events(storage, last_events, events, stats)
    else:
        logger.warning(f"Model is out of date, Further processing has stopped")


def full_update(msg):
    """Apply the events on the current dataset

    :param msg: the result of the application of the events
    :return: Result message
    """
    logger.configure(msg, "UPDATE")
    logger.info(f"Update to GOB Database {GOBStorageHandler.user_name} started")

    # Interpret the message header
    message = ImportMessage(msg)
    metadata = message.metadata

    storage = GOBStorageHandler(metadata)

    # Get events from message
    events = msg["contents"]

    # Gather statistics of update process
    stats = UpdateStatistics()

    _process_events(storage, events, stats)

    # Build result message
    results = stats.results()

    stats.log()
    logger.info(f"Update completed", {'data': results})

    results.update({
        'warnings': logger.get_warnings(),
        'errors': logger.get_errors()
    })

    # Return the result message, with no log, no contents but pass-through any confirms
    message = {
        "header": msg["header"],
        "summary": results,
        "contents": None,
        "confirms": msg.get('confirms')
    }
    return message
