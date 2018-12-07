"""Compare new data with the existing data

Derive Add, Change, Delete and Confirm events by comparing a full set of new data against the full set of current data

Todo: Event, action and mutation are used for the same subject. Use one name to improve maintainability.

"""
from gobcore.events import get_event_for
from gobcore.events.import_message import ImportMessage
from gobcore.log import get_logger
from gobcore.model import GOBModel
from gobcore.typesystem import get_modifications

from gobupload import get_report
from gobupload.storage.handler import GOBStorageHandler

logger = None


def compare(msg):
    """Compare new data in msg (contents) with the current data

    :param msg: The new data, including header and summary
    :return: result message
    """
    global logger
    if logger is None:
        logger = get_logger(name="COMPARE")

    extra_log_kwargs = {
        'process_id': msg['header']['process_id'],
        'source': msg['header']['source'],
        'application': msg['header']['application'],
        'catalogue': msg['header']['catalogue'],
        'entity': msg['header']['entity']
    }
    logger.info(f"Compare to GOB Database {GOBStorageHandler.user_name} started", extra=extra_log_kwargs)

    # Parse the message header
    message = ImportMessage(msg)
    metadata = message.metadata

    gob_model = GOBModel()
    entity_model = gob_model.get_collection(metadata.catalogue, metadata.entity)

    # Read new content into dictionary
    new_entities = {data['_source_id']: data for data in msg["contents"]}

    # Get all current non-deleted ids for this source
    storage = GOBStorageHandler(metadata)
    with storage.get_session():
        # Check any dependencies
        depends_on = msg["header"].get("depends_on", {})
        for key, value in depends_on.items():
            # Check every dependency
            if not storage.has_any_entity(key, value):
                logger.error(f"Compare failed; dependency {value} not found.", extra=extra_log_kwargs)
                return None

        current_ids = storage.get_current_ids()

        # find deletes by comparing current ids to new entities
        # if a current_id is not found in the new_entities it is interpreted as a deletion
        deleted = {current._source_id: None for current in current_ids if current._source_id not in new_entities}

        # combine new and deleted into one set
        all = {**new_entities, **deleted}

        events = []
        for entity_id, data in all.items():
            # get the entity from the storage (or None if it doesn't exist)
            entity = storage.get_current_entity(data)
            # calculate modifications, this will be an empty list if either data or entity is empty
            # or if all attributes are equal
            modifications = get_modifications(entity, data, entity_model['fields'])
            # construct the event given the entity, data, and metadata
            event = get_event_for(entity, data, metadata, modifications)
            # append the event to the events-list to be outputted
            events.append(event)

    results = get_report(events)
    logger.info(f"{results['num_records']} number of events created from message",
                extra={**extra_log_kwargs, 'data': results})

    # Return the result without log.
    return ImportMessage.create_import_message(msg["header"], None, events)
