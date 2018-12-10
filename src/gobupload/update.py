"""Update the current data

Process events and apply the event on the current state of the entity
"""
import json

from gobcore.events.import_message import ImportMessage, MessageMetaData
from gobcore.events import GobEvent
from gobcore.log import get_logger

from gobupload.storage.handler import GOBStorageHandler

logger = None


class Logger():

    _logger = None

    def __init__(self, name, default_args):
        self._name = name
        self._default_args = default_args
        if Logger._logger is None:
            Logger._logger = get_logger("UPDATE")

    def info(self, msg, kwargs={}):
        Logger._logger.info(msg, extra={**(self._default_args), **kwargs})

    def warning(self, msg, kwargs={}):
        Logger._logger.warning(msg, extra={**(self._default_args), **kwargs})


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


def _apply_events(storage, start_after):
    """Apply any unhandled events to the database

    :param storage: GOB (events + entities)
    :param start_after: the is of the last event that has been applied to the storage
    :return:
    """
    with storage.get_session():
        # Get the unhandled events
        unhandled_events = storage.get_events_starting_after(start_after)

        # Log the number of events that is going to be applied (if any)
        n_events = len(unhandled_events)
        n_applied = {}
        if n_events <= 0:
            return n_applied

        logger.info(f"About to apply {n_events} events")

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

            n_applied[event.action] = n_applied.get(event.action, 0) + 1

        for action, n in n_applied.items():
            logger.info(f"{n} {action} events applied")
        return n_applied


def _get_event_ids(storage):
    """Get the highest event id from the entities and the eventid of the most recent event

    :param storage: GOB (events + entities)
    :return:highest entity eventid and last eventid
    """
    with storage.get_session():
        entity_max_eventid = storage.get_entity_max_eventid()
        last_eventid = storage.get_last_eventid()
        return entity_max_eventid, last_eventid


def update_events(storage, message):
    """Store events in GOB

    Only valid events are stored, other events are skipped (with an associated warning)

    :param storage: GOB (events + entities)
    :param message: the event message
    :return:
    """
    with storage.get_session():
        logger.info(f"About to create {len(message.contents)} events")
        n_stored = {}
        n_skipped = {}

        for event in message.contents:
            source_id = event["data"]["_entity_source_id"]
            entity = storage.get_entity_or_none(source_id)

            action = event['event']

            # Is the comparison that has lead to this event based upon the current version of the entity?
            last_event = event["data"]["_last_event"]
            is_valid = last_event is None if entity is None else entity._last_event == last_event
            if is_valid:
                # Store the event in the database
                storage.add_event_to_storage(event)
                n_stored[action] = n_stored.get(action, 0) + 1
            else:
                # Report warning
                logger.warning(f"Skip outdated {action} event, source id: {source_id}",
                               {"id": "Skip outdated event", "data": {"action": action, "source_id": source_id}})
                n_skipped[action] = n_skipped.get(action, 0) + 1

        for action, n in n_stored.items():
            logger.info(f"{n} {action} events created")
        for action, n in n_skipped.items():
            logger.info(f"{n} {action} events skipped")
        return n_stored, n_skipped


def _init_logger(msg):
    """Provide for a logger for this message

    :param msg: the processed message
    :return: None
    """
    global logger

    default_args = {
        'process_id': msg['header']['process_id'],
        'source': msg['header']['source'],
        'catalogue': msg['header']['catalogue'],
        'entity': msg['header']['entity']
    }
    logger = Logger("UPDATE", default_args)


def full_update(msg):
    """Apply the events on the current dataset

    :param msg: the result of the application of the events
    :return: Result message
    """
    _init_logger(msg)

    logger.info(f"Update records to GOB Database {GOBStorageHandler.user_name} started")

    # Interpret the message header
    message = ImportMessage(msg)
    metadata = message.metadata

    storage = GOBStorageHandler(metadata)

    # Get the max eventid of the entities and the last eventid of the events
    entity_max_eventid, last_eventid = _get_event_ids(storage)

    if entity_max_eventid == last_eventid:
        logger.info(f"Model is up to date")
    else:
        logger.warning(f"Model is out of date! Start application of unhandled events")

    # Apply any yet unapplied events
    _apply_events(storage, entity_max_eventid)

    # Add new events
    n_stored, n_skipped = update_events(storage, message)

    # Get the max eventid of the entities and the last eventid of the events
    entity_max_eventid, last_eventid = _get_event_ids(storage)

    # Apply the new events
    n_events_applied = _apply_events(storage, entity_max_eventid)

    # Build result message
    results = {
        "num_records": len(message.contents),
    }
    for result, fmt in [(n_stored, "num_{action}_events"),
                        (n_skipped, "num_{action}_events_skipped"),
                        (n_events_applied, "num_{action}_applied")]:
        for action, n in result.items():
            results[fmt.format(action=action.lower())] = n

    logger.info(f"Update completed", {'data': results})

    # Return the result message, with no log, no contents
    return ImportMessage.create_import_message(msg["header"], None, None)
