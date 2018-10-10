"""Update the current data

Process events and apply the event on the current state of the entity
"""
from gobcore.events.import_message import ImportMessage
from gobcore.events import GobEvent
from gobcore.log import get_logger

from gobuploadservice import get_report
from gobuploadservice.storage.handler import GOBStorageHandler


logger = get_logger(name="UPDATE")


def full_update(msg):
    """Apply the events on the current dataset

    :param msg: the result of the application of the events
    :return:
    """
    extra_log_kwargs = {
        'process_id': msg['header']['process_id'],
        'source': msg['header']['source'],
        'entity': msg['header']['entity']
    }
    logger.info("Update records to GOB Database started", extra=extra_log_kwargs)

    # Interpret the message header
    message = ImportMessage(msg)
    metadata = message.metadata

    storage = GOBStorageHandler(metadata)

    with storage.get_session():
        for event in message.contents:
            # Store the event in the database
            storage.add_event_to_storage(event)

            # Get the gob_event
            gob_event = GobEvent(event, metadata)

            # read and remove relevant id's (requires `get_event_for` in `compare` to put them in)
            # todo: currently source_id is always the same as the entity_id. If this will always  be true,
            #       then we can skip one of the two.
            entity_id, source_id = gob_event.pop_ids()

            # Updates on entities are uniquely identified by the source_id
            entity = storage.get_entity_for_update(entity_id, source_id, gob_event)

            # apply the event on the entity
            gob_event.apply_to(entity)

    results = get_report(message.contents)
    logger.info(f"{results['RECORDS']} number of events applied to database",
                extra={**extra_log_kwargs, 'data': results})

    # Return the result message, with no log, no contents
    return ImportMessage.create_import_message(metadata.as_header, None, None)
