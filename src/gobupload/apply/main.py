import sys
from pathlib import Path
from typing import Sequence

from sqlalchemy.engine import Row

from gobcore.events.import_events import CONFIRM
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.message_broker.notifications import EventNotification, add_notification
from gobcore.utils import ProgressTicker

from gobupload.config import FULL_UPLOAD
from gobupload.storage.handler import GOBStorageHandler
from gobupload.apply.event_applicator import EventApplicator
from gobupload.update.update_statistics import UpdateStatistics
from gobupload.utils import get_event_ids, is_corrupted

# Trigger VACUUM ANALYZE on database if more than ANALYZE_THRESHOLD of entities are updated. When ANALYZE_THRESHOLD =
# 0.3, this means that if more than 30% of the events update the data (MODIFY's, ADDs, DELETEs), a VACUUM ANALYZE is
# triggered.
ANALYZE_THRESHOLD = 0.3


def apply_events(storage: GOBStorageHandler, last_events: set[str], start_after: int, stats: UpdateStatistics):
    """Apply any unhandled events to the database

    :param storage: GOB (events + entities)
    :param last_events: all entities with events applied
    :param start_after: the is of the last event that has been applied to the storage
    :param stats: update statitics for this action
    :return:
    """
    CHUNK_SIZE = 10_000

    with (
        ProgressTicker("Apply events", CHUNK_SIZE) as progress,
        EventApplicator(storage, last_events, stats) as event_applicator,
    ):
        while chunk := storage.get_events_starting_after(start_after, CHUNK_SIZE):
            with storage.get_session():
                for event in chunk:
                    progress.tick()
                    event_applicator.load(event)

                event_applicator.flush()
                start_after = event.eventid


def _apply_confirms(storage: GOBStorageHandler, confirms_table: str, stats: UpdateStatistics):
    update_size = 25_000

    with ProgressTicker("Apply CONFIRM events", update_size) as progress:
        while True:
            updated_rows = storage.apply_confirms(confirms_table, progress._count, update_size)
    
            progress.ticks(updated_rows)
            stats.add_applied(CONFIRM.name, updated_rows)
    
            if updated_rows < update_size:
                break


def apply_confirm_events(storage: GOBStorageHandler, stats: UpdateStatistics, msg: dict):
    """
    Apply confirm events (if present)

    (BULK)CONFIRM events can be passed in a file.
    The name of the file is mag['confirms'].

    :param storage:
    :param stats:
    :param msg:
    :return:
    """
    if not msg.get("confirms"):
        return

    confirms = Path(msg["confirms"])
    timestamp = msg["header"]["timestamp"]

    try:
        with storage.get_session(invalidate=True):
            table = storage.create_confirms_table(confirms, timestamp)
            _apply_confirms(storage, table, stats)
    finally:
        confirms.unlink(missing_ok=True)
        del msg["confirms"]


def _should_analyze(stats):
    applied_stats = stats.get_applied_stats()
    return (1 - applied_stats.get('CONFIRM', {}).get('relative', 0)) > ANALYZE_THRESHOLD and \
        sum([value['absolute'] for value in applied_stats.values()]) > 0


def _get_source_catalog_entity_combinations(msg) -> Sequence[Row]:
    header = msg["header"]
    storage = GOBStorageHandler(only=[GOBStorageHandler.EVENTS_TABLE])

    catalogue = header.get("catalogue")
    entity = header.get("entity") or header.get("collection")

    if catalogue is None and entity is None:
        # we should not query event table without filters
        raise GOBException(f"No catalogue or collection specified in header: {header}")

    return storage.get_source_catalogue_entity_combinations(catalogue, entity, source=header.get("source"))


def apply(msg):
    mode = msg['header'].get('mode', FULL_UPLOAD)

    logger.info("Apply events")

    # Gather statistics of update process
    stats = UpdateStatistics()
    before = None
    after = None

    for result in _get_source_catalog_entity_combinations(msg):
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
            last_events = set(storage.get_current_ids(exclude_deleted=False))

            apply_events(storage, last_events, entity_max_eventid, stats)
            apply_confirm_events(storage, stats, msg)

        # Track eventId after event application
        entity_max_eventid, last_eventid = get_event_ids(storage)
        after = max(entity_max_eventid or 0, after or 0)

        # Build result message
        results = stats.results()

        logger.info("Running VACUUM ANALYZE on table")
        storage.analyze_table()

        stats.log()
        logger.info(f"Apply events {model} completed", {'data': results})

    msg['summary'] = logger.get_summary()

    # Add events notification telling what types of event have been applied
    if not msg['header'].get('suppress_notifications', False) and before is not None:  # before is None: nothing done
        add_notification(msg, EventNotification(stats.applied, [before, after]))

    return msg
