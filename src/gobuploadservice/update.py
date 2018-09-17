"""Update the current data

Process events and apply the event on the current state of the entity
"""
from gobcore.events.import_message import ImportMessage
from gobcore.events import GobEvent

from gobuploadservice import print_report
from gobuploadservice.storage.storage_handler import GOBStorageHandler


def full_update(msg):
    """Apply the events on the current dataset

    :param msg: the result of the application of the events
    :return:
    """
    # Interpret the message header

    message = ImportMessage(msg)
    metadata = message.metadata

    db = GOBStorageHandler(metadata)

    with db.get_session():
        for event in message.contents:
            # Store the event in the database
            db.add_event_to_db(event)

            # Get the gob_event
            gob_event = GobEvent(event, metadata)

            # read and remove relevant id's (requires `get_event_for` in `compare` to put them in)
            # todo: currently source_id is always the same as the entity_id. If this will always  be true,
            #       then we can skip one of the two.
            entity_id, source_id = gob_event.pop_ids()

            # Updates on entities are uniquely identified by the source_id
            entity = db.get_entity_for_update(entity_id, source_id, gob_event)

            # apply the event on the entity
            gob_event.apply_to(entity)

    print_report(message.contents)

    # Return the result message, with no log, no contents
    return ImportMessage.create_import_message(metadata.as_header, None, None)
