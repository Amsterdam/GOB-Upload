"""Update the current data

Process events and apply the event on the current state of the entity
"""
import json

from gobcore.events.import_message import ImportMessage, MessageMetaData
from gobcore.events import GobEvent

from gobupload import logger
from gobupload.storage.handler import GOBStorageHandler
from gobupload.compare import recompare


class UpdateStatistics():

    def __init__(self, events, recompares):
        self.stored = {}
        self.skipped = {}
        self.applied = {}
        self.events = events
        self.recompares = recompares

    def add_stored(self, action):
        self.stored[action] = self.stored.get(action, 0) + 1

    def add_skipped(self, action):
        self.skipped[action] = self.skipped.get(action, 0) + 1

    def add_applied(self, action):
        self.applied[action] = self.applied.get(action, 0) + 1

    def results(self):
        """Get statistics in a dictionary

        :return:
        """
        results = {
            "num_events": len(self.events)
        }
        if self.recompares:
            results["num_recompares"] = len(self.recompares)
        for result, fmt in [(self.stored, "num_{action}_events"),
                            (self.skipped, "num_{action}_events_skipped"),
                            (self.applied, "num_{action}_applied")]:
            for action, n in result.items():
                results[fmt.format(action=action.lower())] = n
        return results

    def log(self):
        for process in ["stored", "skipped", "applied"]:
            for action, n in getattr(self, process).items():
                logger.info(f"{n} {action} events {process}")


def _get_gob_event(event, data):
    """Reconstruct the original event out of the stored event

    :param event: the database event
    :param data: the data that is associated with the event
    :return: a ADD, MODIFY, CONFIRM or DELETE event
    """

    event_msg = {
        "event": event.action,
        "data": data
    }

    msg_header = {
        "process_id": None,
        "source": event.source,
        "application": event.application,
        "id_column": data.get("id_column"),
        "catalogue": event.catalogue,
        "entity": event.entity,
        "version": event.version,
        "timestamp": event.timestamp
    }

    # Construct the event out of the reconstructed event data
    gob_event = GobEvent(event_msg, MessageMetaData(msg_header))

    return gob_event


def _apply_events(storage, start_after, stats):
    """Apply any unhandled events to the database

    :param storage: GOB (events + entities)
    :param start_after: the is of the last event that has been applied to the storage
    :param stats: update statitics for this action
    :return:
    """
    with storage.get_session():
        # Get the unhandled events
        unhandled_events = storage.get_events_starting_after(start_after)

        # Log the number of events that is going to be applied (if any)
        n_events = len(unhandled_events)
        applied = {}
        if n_events <= 0:
            return applied

        logger.info(f"Apply {n_events} events")

        for event in unhandled_events:

            # Parse the json data of the event
            data = json.loads(event.contents)

            # Get the entity to which the event should be applied, create if ADD event
            entity = storage.get_entity_for_update(event, data)

            # Reconstruct the gob event out of the database event
            gob_event = _get_gob_event(event, data)

            # apply the event on the entity
            gob_event.apply_to(entity)

            # and register the last event that has updated this entity
            entity._last_event = event.eventid

            # Collect statistics about the actions that have been applied
            stats.add_applied(event.action)


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

    :param storage: GOB (events + entities)
    :param events: the events to process
    :param stats: update statitics for this action
    :return:
    """
    with storage.get_session():
        logger.info(f"Create {len(events)} events")

        for event in events:
            _store_event(storage, event, stats)


def _store_event(storage, event, stats):
    """Store the events

    Only store valid events (not outdated)

    :param storage: GOB (events + entities)
    :param event: the event to process
    :param stats: update statitics for this action
    :return:
    """
    source_id = event["data"]["_entity_source_id"]
    entity = storage.get_entity_or_none(source_id)

    action = event['event']

    # Is the comparison that has lead to this event based upon the current version of the entity?
    last_event = event["data"]["_last_event"]
    is_valid = last_event is None if entity is None else entity._last_event == last_event

    if is_valid:
        # Store the event in the database
        storage.add_event_to_storage(event)
        stats.add_stored(action)
    else:
        # Report warning
        logger.warning(f"Skip outdated {action} event, source id: {source_id}",
                       {"id": "Skip outdated event", "data": {"action": action, "source_id": source_id}})
        stats.add_skipped(action)


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
    logger.info(f"Update records to GOB Database {GOBStorageHandler.user_name} started")

    # Interpret the message header
    message = ImportMessage(msg)
    metadata = message.metadata

    storage = GOBStorageHandler(metadata)

    # Get events and recompares from message
    events = message.contents["events"]
    recompares = message.contents["recompares"]

    # Gather statistics of update process
    stats = UpdateStatistics(events, recompares)

    logger.info(f"Process {len(events)} events")
    _process_events(storage, events, stats)

    if recompares:
        logger.info(f"Process {len(recompares)} recompares")
        for data in recompares:
            with storage.get_session():
                event = recompare(storage, data)
            _process_events(storage, [event], stats)

    # Build result message
    results = stats.results()

    stats.log()
    logger.info(f"Update completed", {'data': results})

    # Return the result message, with no log, no contents
    return ImportMessage.create_import_message(msg["header"], None, None)
