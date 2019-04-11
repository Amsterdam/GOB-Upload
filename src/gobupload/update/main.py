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


def _apply_events(storage, start_after, stats):
    """Apply any unhandled events to the database

    :param storage: GOB (events + entities)
    :param start_after: the is of the last event that has been applied to the storage
    :param stats: update statitics for this action
    :return:
    """
    with storage.get_session():
        logger.info(f"Apply events")

        event_applicator = EventApplicator(storage)

        with ProgressTicker("Apply events", 10000) as progress:
            unhandled_events = storage.get_events_starting_after(start_after)
            for event in unhandled_events:
                progress.tick()

                action, count = event_applicator.apply(event)
                stats.add_applied(action, count)


def _get_event_ids(storage):
    """Get the highest event id from the entities and the eventid of the most recent event

    :param storage: GOB (events + entities)
    :return:highest entity eventid and last eventid
    """
    with storage.get_session():
        entity_max_eventid = storage.get_entity_max_eventid()
        last_eventid = storage.get_last_eventid()
        return entity_max_eventid, last_eventid


def _store_events(storage, events, stats):
    """Store events in GOB

    Only valid events are stored, other events are skipped (with an associated warning)
    The events are added in bulk in the database

    :param storage: GOB (events + entities)
    :param events: the events to process
    :param stats: update statitics for this action
    :return:
    """
    with storage.get_session():
        # Use a session to commit all or rollback on any error
        logger.info(f"Store events")

        event_collector = EventCollector(storage)

        with ProgressTicker("Store events", 10000) as progress:
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
    entity_max_eventid, last_eventid = _get_event_ids(storage)

    if entity_max_eventid == last_eventid:
        logger.info(f"Model is up to date")
    elif entity_max_eventid and not last_eventid or entity_max_eventid > last_eventid:
        logger.error(f"Model is inconsistent! data is more recent than events")
        return
    else:
        logger.warning(f"Model is out of date! Start application of unhandled events")
        _apply_events(storage, entity_max_eventid, stats)
        logger.error(f"Further processing has stopped")
        # New events will almost certainly be invalid. So stop further processing
        return

    # Add new events
    _store_events(storage, events, stats)

    # Get the max eventid of the entities and the last eventid of the events
    entity_max_eventid, last_eventid = _get_event_ids(storage)

    # Apply the new events
    _apply_events(storage, entity_max_eventid, stats)


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
    events = message.contents["events"]

    # Gather statistics of update process
    stats = UpdateStatistics()

    _process_events(storage, events, stats)

    # Build result message
    results = stats.results()

    stats.log()
    logger.info(f"Update completed", {'data': results})

    # Return the result message, with no log, no contents
    return ImportMessage.create_import_message(msg["header"], None, None)