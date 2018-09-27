"""Compare new data with the existing data

Derive Add, Change, Delete and Confirm events by comparing a full set of new data against the full set of current data

Todo: Event, action and mutation are used for the same subject. Use one name to improve maintainability.

"""
from gobcore.events import get_event_for
from gobcore.events.import_message import ImportMessage
from gobcore.typesystem import get_modifications

from gobuploadservice import print_report
from gobuploadservice.storage.handler import GOBStorageHandler


def compare(msg):
    """Compare new data in msg (contents) with the current data

    :param msg: The new data, including header and summary
    :return: result message
    """

    # Parse the message header
    message = ImportMessage(msg)
    metadata = message.metadata

    # Read new content into dictionary
    new_entities = {data['_source_id']: data for data in msg["contents"]}

    # Get all current non-deleted ids for this source
    storage = GOBStorageHandler(metadata)
    with storage.get_session():
        current_ids = storage.get_current_ids()

        # find deletes by comparing current ids to new entities
        # if a current_id is not found in the new_entities it is interpreted as a deletion
        deleted = {current._source_id: None for current in current_ids if current._source_id not in new_entities}

        # combine new and deleted into one set
        all = {**new_entities, **deleted}

        events = []
        for entity_id, data in all.items():
            # get the entity from the storage (or None if it doesn't exist)
            entity = storage.get_entity_or_none(entity_id)
            # calculate modifications, this will be an empty list if either data or entity is empty
            # or if all attributes are equal
            modifications = get_modifications(entity, data, metadata.model)
            # construct the event given the entity, data, and metadata
            event = get_event_for(entity, data, metadata, modifications)
            # append the event to the events-list to be outputted
            events.append(event)

    print_report(events)

    # Return the result without log.
    return ImportMessage.create_import_message(metadata.as_header, None, events)
