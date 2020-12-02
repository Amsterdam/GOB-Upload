import os
import sys

from typing import List

from gobcore.events.import_events import ImportEvent
from gobcore.logging.logger import logger
from gobcore.message_broker.async_message_broker import AsyncConnection as MessageBrokerConnection
from gobcore.message_broker.config import CONNECTION_PARAMS, EVENT_EXCHANGE
from gobcore.message_broker.events import get_routing_key
from gobcore.message_broker.notifications import EventNotification, add_notification
from gobcore.message_broker.offline_contents import ContentsReader
from gobcore.utils import ProgressTicker

from gobupload.config import FULL_UPLOAD
from gobupload.storage.handler import GOBStorageHandler
from gobupload.update.event_applicator import EventApplicator
from gobupload.update.update_statistics import UpdateStatistics
from gobupload.utils import ActiveGarbageCollection, get_event_ids, is_corrupted

# Trigger VACUUM ANALYZE on database if more than ANALYZE_THRESHOLD of entities are updated. When ANALYZE_THRESHOLD =
# 0.3, this means that if more than 30% of the events update the data (MODIFY's, ADDs, DELETEs), a VACUUM ANALYZE is
# triggered.
ANALYZE_THRESHOLD = 0.3


def _broadcast_events(message_broker_connection: MessageBrokerConnection, events: List[ImportEvent]):
    """Broadcasts list of ImportEvents as batch to EVENT_EXCHANGE with corresponding routing key (depending
    on catalog/collection)

    :param message_broker_connection:
    :param event:
    :return:
    """

    to_send = {}

    for event in events:
        key = get_routing_key(event.catalogue, event.entity)

        if key not in to_send:
            to_send[key] = []

        event_msg = {
            'header': {
                'event_id': event.id,
                'last_event_id': event.last_event,
                'source_id': event._data.get('_entity_source_id', event._data['_source_id']),
                'name': event.name,
                'type': event.action,
                'catalog': event.catalogue,
                'collection': event.entity,
                'source': event.source,
            },
            'contents': event._data,
        }
        to_send[key].append(event_msg)

    for key, events in to_send.items():
        message_broker_connection.publish(EVENT_EXCHANGE, key, {'contents': events})


def apply_events(storage, last_events, start_after, stats):
    """Apply any unhandled events to the database

    :param storage: GOB (events + entities)
    :param start_after: the is of the last event that has been applied to the storage
    :param stats: update statitics for this action
    :return:
    """
    with ActiveGarbageCollection("Apply events"), storage.get_session(), MessageBrokerConnection(
            CONNECTION_PARAMS) as messagebroker_connection:
        logger.info(f"Apply events")

        PROCESS_PER = 10000
        with ProgressTicker("Apply events", PROCESS_PER) as progress:
            unhandled_events = storage.get_events_starting_after(start_after, PROCESS_PER)
            while unhandled_events:
                with EventApplicator(storage, last_events) as event_applicator:
                    for event in unhandled_events:
                        progress.tick()

                        gob_event, count, applied_events = event_applicator.apply(event)
                        action = gob_event.action
                        stats.add_applied(action, count)
                        start_after = event.eventid

                        if applied_events:
                            _broadcast_events(
                                messagebroker_connection,
                                applied_events
                            )
                    applied_events = event_applicator.apply_all()
                    if applied_events:
                        _broadcast_events(
                            messagebroker_connection,
                            applied_events
                        )

                unhandled_events = storage.get_events_starting_after(start_after, PROCESS_PER)


def apply_confirm_events(storage, stats, msg):
    """
    Apply confirm events (if present)

    (BULK)CONFIRM events can be passed in a file.
    The name of the file is mag['confirms'].

    :param storage:
    :param stats:
    :param msg:
    :return:
    """
    confirms = msg.get('confirms')
    # SKIP confirms for relations
    catalogue = msg['header'].get('catalogue', "")
    if confirms and catalogue != 'rel':
        reader = ContentsReader(confirms)
        with ProgressTicker("Apply CONFIRM events", 10000) as progress:
            for event in reader.items():
                progress.tick()
                action = event['event']
                assert action in ['CONFIRM', 'BULKCONFIRM']
                # get confirm data: BULKCONFIRM => data.confirms, CONFIRM => [data]
                confirm_data = event['data'].get('confirms', [event['data']])
                storage.apply_confirms(confirm_data, msg['header']['timestamp'])
                stats.add_applied('CONFIRM', len(confirm_data))
        reader.close()
    if confirms:
        # Remove file after it has been handled (or skipped)
        os.remove(confirms)
        del msg['confirms']


def _should_analyze(stats):
    applied_stats = stats.get_applied_stats()
    return (1 - applied_stats.get('CONFIRM', {}).get('relative', 0)) > ANALYZE_THRESHOLD and \
        sum([value['absolute'] for value in applied_stats.values()]) > 0


def _get_source_catalog_entity_combinations(storage, msg):
    source = msg['header'].get('source')
    catalogue = msg['header'].get('catalogue', "")
    entity = msg['header'].get('entity', "")

    combinations = storage.get_source_catalogue_entity_combinations(catalogue=catalogue, entity=entity)
    # Apply for all sources if source is None or apply only the specified source
    return [combination for combination in combinations if source is None or source == combination.source]


def apply(msg):
    mode = msg['header'].get('mode', FULL_UPLOAD)

    logger.configure(msg, "UPDATE")
    logger.info(f"Apply events")

    storage = GOBStorageHandler()
    combinations = _get_source_catalog_entity_combinations(storage, msg)

    # Gather statistics of update process
    stats = UpdateStatistics()
    before = None
    after = None
    for result in combinations:
        model = f"{result.source} {result.catalogue} {result.entity}"
        logger.info(f"Apply events {model}")
        storage = GOBStorageHandler(result)

        # Track eventId before event application
        entity_max_eventid, last_eventid = get_event_ids(storage)
        before = min(entity_max_eventid or 0, before or sys.maxsize)

        if is_corrupted(entity_max_eventid, last_eventid):
            logger.error(f"Model {model} is inconsistent! data is more recent than events")
        elif entity_max_eventid == last_eventid:
            logger.info(f"Model {model} is up to date")
            apply_confirm_events(storage, stats, msg)
        else:
            logger.info(f"Start application of unhandled {model} events")
            with storage.get_session():
                last_events = storage.get_last_events()  # { source_id: last_event, ... }

            apply_events(storage, last_events, entity_max_eventid, stats)
            apply_confirm_events(storage, stats, msg)

        # Track eventId after event application
        entity_max_eventid, last_eventid = get_event_ids(storage)
        after = max(entity_max_eventid or 0, after or 0)

        # Build result message
        results = stats.results()
        if mode == FULL_UPLOAD and _should_analyze(stats):
            logger.info(f"Running VACUUM ANALYZE on table")
            storage.analyze_table()

        stats.log()
        logger.info(f"Apply events {model} completed", {'data': results})

    msg['summary'] = logger.get_summary()

    # Add a events notification telling what types of event have been applied
    if not msg['header'].get('suppress_notifications', False):
        add_notification(msg, EventNotification(stats.applied, [before, after]))

    return msg
