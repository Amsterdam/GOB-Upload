"""Update the current data

Process events and apply the event on the current state of the entity
"""
import json

from gobcore.events.import_message import ImportMessage, MessageMetaData
from gobcore.events import GobEvent, GOB
from gobcore.logging.logger import logger

from gobupload.storage.handler import GOBStorageHandler


class UpdateStatistics():

    def __init__(self, events):
        self.stored = {}
        self.skipped = {}
        self.applied = {}
        self.bulkconfirm_stored = None
        self.bulkconfirm_applied = None
        self.events = events

    def add_stored(self, action):
        self.stored[action] = self.stored.get(action, 0) + 1

    def add_skipped(self, action):
        self.skipped[action] = self.skipped.get(action, 0) + 1

    def add_applied(self, action):
        self.applied[action] = self.applied.get(action, 0) + 1

    def add_bulkconfirm_applied(self, num_records):
        self.bulkconfirm_applied = num_records

    def add_bulkconfirm_stored(self, num_records):
        self.bulkconfirm_stored = num_records

    def results(self):
        """Get statistics in a dictionary

        :return:
        """
        results = {
            "num_events": len(self.events)
        }
        for result, fmt in [(self.stored, "num_{action}_events"),
                            (self.skipped, "num_{action}_events_skipped"),
                            (self.applied, "num_{action}_applied")]:
            for action, n in result.items():
                results[fmt.format(action=action.lower())] = n

        if self.bulkconfirm_stored:
            results['bulkconfirm_records_stored'] = self.bulkconfirm_stored

        if self.bulkconfirm_applied:
            results['bulkconfirm_records_applied'] = self.bulkconfirm_applied

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

    # Store the id of the event in the gob_event
    gob_event.id = event.eventid
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

        # Get the list of current entities by _source_id
        entities = storage.get_last_events()

        # Log the number of events that is going to be applied (if any)
        n_events = len(unhandled_events)
        applied = {}
        if n_events <= 0:
            return applied

        logger.info(f"Apply {n_events} events")

        new_add_events = []

        for event in unhandled_events:
            # Parse the json data of the event
            data = json.loads(event.contents)
            # Reconstruct the gob event out of the database event
            gob_event = _get_gob_event(event, data)

            if isinstance(gob_event, GOB.BULKCONFIRM):
                storage.bulk_update_confirms(gob_event, event.eventid)
                stats.add_bulkconfirm_applied(len(gob_event._data['confirms']))
            else:
                # Save new ADD events to insert in bulk
                if data['_entity_source_id'] not in entities:
                    new_add_events.append(gob_event)
                else:
                    # Get the entity to which the event should be applied, create if ADD event
                    entity = storage.get_entity_for_update(event, data)

                    # apply the event on the entity
                    gob_event.apply_to(entity)

                    # and register the last event that has updated this entity
                    entity._last_event = event.eventid

            # Collect statistics about the actions that have been applied
            stats.add_applied(event.action)

        if new_add_events:
            # Process bulk_add_events
            storage.bulk_add_entities(new_add_events)


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
        logger.info(f"Create {len(events)} events")

        entities = storage.get_last_events()

        # If there are no entities in the DB, we can apply the ADD events without evaluating _last_event
        if len(entities) == 0:
            storage.bulk_add_events(events)
            return

        valid_events = []
        for event in events:
            event_type = event['event']
            source_id = event['data']['_entity_source_id']

            if _validate_event(entities, event, stats):
                valid_events.append(event)
                stats.add_stored(event_type)
                if(event_type == 'BULKCONFIRM'):
                    stats.add_bulkconfirm_stored(len(event['data']['confirms']))
            else:
                logger.warning(f"Skip outdated {event_type} event",
                               {
                                    "id": "Skip outdated event",
                                    "data": {
                                        "action": "{event_type}",
                                        "source_id": source_id
                                    }
                                })
                stats.add_skipped(event_type)

        # Insert all valid events in bulk to the database
        storage.bulk_add_events(valid_events)


def _validate_event(entities, event, stats):
    """Validate an event to the current entities and add the statistics

    :param entities: a list of current entities with _source_id and _last_event
    :param event: the event to validate
    :param stats: the UpdateStatistics instance to register stats with
    :return: if the event is valid according to it's _last_event
    """
    event_type = event['event']
    valid = False

    if event_type == 'BULKCONFIRM':
        for confirm in event['data']['confirms']:
            source_id = confirm['_source_id']
            last_event = confirm['_last_event']
            # If the last_event doesn't match, remove from confirms in BULKCONFIRM
            if last_event != entities.get(source_id, None):
                event['data']['confirms'].remove(confirm)
                logger.warning(f"Skip outdated record in BULKCONFIRM event, source id: {source_id}",
                               {
                                    "id": "Skip outdated record in BULKCONFIRM event",
                                    "data": {
                                        "action": "CONFIRM", "source_id": source_id
                                    }
                                })
            # Event is valid if there are still records in the BULKCONFIRM
            if len(event['data']) > 0:
                valid = True
    else:
        source_id = event['data']['_entity_source_id']
        last_event = event['data']['_last_event']

        # Check if last_events matches the entities last_event
        if last_event == entities.get(source_id, None):
            valid = True
    return valid


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

    # Get events from message
    events = message.contents["events"]

    # Gather statistics of update process
    stats = UpdateStatistics(events)

    logger.info(f"Process {len(events)} events")
    _process_events(storage, events, stats)

    # Build result message
    results = stats.results()

    stats.log()
    logger.info(f"Update completed", {'data': results})

    # Return the result message, with no log, no contents
    return ImportMessage.create_import_message(msg["header"], None, None)
