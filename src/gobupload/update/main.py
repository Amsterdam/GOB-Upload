"""Update the current data

Process events and apply the event on the current state of the entity
"""
from gobcore.events.import_message import ImportMessage
from gobcore.logging.logger import logger
from gobcore.utils import ProgressTicker

from gobupload.storage.handler import GOBStorageHandler
from gobupload.update.update_statistics import UpdateStatistics
from gobupload.update.event_collector import EventCollector
from gobupload.update.event_applicator import EventApplicator
from gobupload.utils import ActiveGarbageCollection


def _apply_events(storage, last_events, start_after, stats):
    """Apply any unhandled events to the database

    :param storage: GOB (events + entities)
    :param start_after: the is of the last event that has been applied to the storage
    :param stats: update statitics for this action
    :return:
    """
    with ActiveGarbageCollection("Apply events"), storage.get_session():
        logger.info(f"Apply events")

        with ProgressTicker("Apply events", 10000) as progress, \
                EventApplicator(storage, last_events) as event_applicator:
            unhandled_events = storage.get_events_starting_after(start_after)
            for event in unhandled_events:
                progress.tick()

                action, count = event_applicator.apply(event)
                stats.add_applied(action, count)

    with ActiveGarbageCollection("Delete any confirm events"), storage.get_session():
        logger.info(f"Post-process any CONFIRM events")

        # Confirms are deleted once they have been applied
        storage.delete_confirms()


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


def _get_event_ids(storage):
    """Get the highest event id from the entities and the eventid of the most recent event

    :param storage: GOB (events + entities)
    :return:highest entity eventid and last eventid
    """
    with storage.get_session():
        entity_max_eventid = storage.get_entity_max_eventid()
        last_eventid = storage.get_last_eventid()
        return entity_max_eventid, last_eventid


def _process_events(storage, events, stats):
    """Store and apply events

    :param storage: GOB (events + entities)
    :param event: the event to process
    :param stats: update statitics for this action
    :return:
    """
    # Get the max eventid of the entities and the last eventid of the events
    entity_max_eventid, last_eventid = _get_event_ids(storage)

    # Get all source_id - last_event combinations to check for validity and existence
    with storage.get_session():
        last_events = storage.get_last_events()  # { source_id: last_event, ... }

    if is_corrupted(entity_max_eventid, last_eventid):
        logger.error(f"Model is inconsistent! data is more recent than events")
    elif entity_max_eventid == last_eventid:
        logger.info(f"Model is up to date")
        # Add new events
        _store_events(storage, last_events, events, stats)
        # Apply the new events
        _apply_events(storage, last_events, entity_max_eventid, stats)
    else:
        logger.warning(f"Model is out of date! Start application of unhandled events")
        _apply_events(storage, last_events, entity_max_eventid, stats)
        logger.error(f"Further processing has stopped")


def is_corrupted(entity_max_eventid, last_eventid):
    if last_eventid is None and entity_max_eventid is None:
        # no events, no entities
        return False
    elif last_eventid is not None and entity_max_eventid is None:
        # events but no data (apply has failed or upload has been aborted)
        return False
    elif entity_max_eventid is not None and last_eventid is None:
        # entities but no events (data is corrupted)
        return True
    elif entity_max_eventid is not None and last_eventid is not None:
        # entities and events, entities can never be newer than events
        return entity_max_eventid > last_eventid


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

    # Return the result message, with no log, no contents
    message = {
        "header": msg["header"],
        "summary": results,
        "contents": None
    }
    return message
