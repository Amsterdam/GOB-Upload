from gobcore.logging.logger import logger
from gobcore.utils import ProgressTicker

from gobupload.storage.handler import GOBStorageHandler
from gobupload.update.update_statistics import UpdateStatistics
from gobupload.update.event_applicator import EventApplicator
from gobupload.utils import ActiveGarbageCollection, is_corrupted, get_event_ids


def apply_events(storage, last_events, start_after, stats):
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


def apply(msg):
    catalogue = msg['header'].get('catalogue', "")
    entity = msg['header'].get('entity', "")

    logger.configure(msg, "UPDATE MODEL")
    logger.info(f"Update model {catalogue} {entity}")

    storage = GOBStorageHandler()
    combinations = storage.get_source_catalogue_entity_combinations(catalogue=catalogue, entity=entity)

    for result in combinations:
        model = f"{result.source} {result.catalogue} {result.entity}"
        storage = GOBStorageHandler(result)
        entity_max_eventid, last_eventid = get_event_ids(storage)
        if is_corrupted(entity_max_eventid, last_eventid):
            logger.error(f"Model {model} is inconsistent! data is more recent than events")
        elif entity_max_eventid == last_eventid:
            logger.info(f"Model {model} is up to date")
        else:
            logger.info(f"Start application of unhandled events")
            with storage.get_session():
                last_events = storage.get_last_events()  # { source_id: last_event, ... }

            # Gather statistics of update process
            stats = UpdateStatistics()

            apply_events(storage, last_events, entity_max_eventid, stats)

            # Build result message
            results = stats.results()

            stats.log()
            logger.info(f"Update model completed", {'data': results})


    msg['summary'] = {
        'warnings': logger.get_warnings(),
        'errors': logger.get_errors()
    }
    return msg
