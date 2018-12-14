"""Compare new data with the existing data

Derive Add, Change, Delete and Confirm events by comparing a full set of new data against the full set of current data

Todo: Event, action and mutation are used for the same subject. Use one name to improve maintainability.

"""
from gobcore.events import get_event_for
from gobcore.events.import_message import ImportMessage
from gobcore.model import GOBModel
from gobcore.typesystem import get_modifications

from gobupload import init_logger
from gobupload import get_report
from gobupload.storage.handler import GOBStorageHandler

logger = None


def compare(msg):
    """Compare new data in msg (contents) with the current data

    :param msg: The new data, including header and summary
    :return: result message
    """
    global logger
    logger = init_logger(msg, "COMPARE")
    logger.info(f"Compare to GOB Database {GOBStorageHandler.user_name} started")

    # Parse the message header
    message = ImportMessage(msg)
    metadata = message.metadata

    # Get the model for the collection to be compared
    gob_model = GOBModel()
    entity_model = gob_model.get_collection(metadata.catalogue, metadata.entity)

    # Initialize a storage handler for the collection
    storage = GOBStorageHandler(metadata)

    with storage.get_session():
        # Check any dependencies
        depends_on = msg["header"].get("depends_on", {})
        for key, value in depends_on.items():
            # Check every dependency
            if not storage.has_any_entity(key, value):
                logger.error(f"Compare failed; dependency {value} not fulfilled.")
                return None

        # Convert msg contents in events
        events, recompares = _process_new_data(storage, entity_model, msg)

        # Derive deletions from msg contents
        events.extend(_process_deletions(storage, entity_model, msg))

    results = get_report(msg["contents"], events, recompares)
    logger.info(f"Message processed", kwargs={'data': results})

    msg_contents = {
        "events": events,
        "recompares": recompares
    }

    # Return the result without log.
    return ImportMessage.create_import_message(msg["header"], None, msg_contents)


def _process_new_data(storage, model, msg):
    """Convert the data in the message into events and recompares

    Recompares occur when the message contains multiple new volgnummers for the same state
    The volgnummers denote modifications or confirms to the state
    They should be processed in order to have a consistent history for the state

    :param storage: Storage handler instance for the collection being processed
    :param model: GOB Model for the collection
    :param msg:
    :return: list of events, list of recompares
    """
    previous_ids = {}
    events = []
    recompares = []

    for data in msg["contents"]:
        event = _compare_new_data(model, storage, new_data=data)
        if event is None:
            # Skip historical states
            continue

        # Check for any multiple new volgnummers, use previous_ids to register volgnummers
        recompare = _get_recompare(model, previous_ids, event, data)
        if recompare is not None:
            recompares.append(recompare)
            continue

        # append the event to the events-list to be outputted
        events.append(event)

    return events, recompares


def _process_deletions(storage, model, msg):
    """Derive deletions

    By comparing stored data with new data

    :param storage: Storage handler instance for the collection being processed
    :param model: GOB Model for the collection
    :param msg:
    :return: list of Delete Events
    """
    # Read new content into dictionary
    new_entities = {data['_source_id']: data for data in msg["contents"]}

    # Retrieve current ids for the same collection
    current_ids = storage.get_current_ids()

    # find deletes by comparing current ids to new entities
    # if a current_id is not found in the new_entities it is interpreted as a deletion
    deleted = {current._source_id: None for current in current_ids if current._source_id not in new_entities}

    events = []
    for entity_id, data in deleted.items():
        events.append(_compare_new_data(model, storage, entity_id=entity_id))
    return events


def _get_recompare(model, previous_ids, event, data):
    """Check for any recompares

    Recompares occur when the message contains multiple new volgnummers for the same state
    The volgnummers denote modifications or confirms to the state
    They should be processed in order to have a consistent history for the state

    If more than 1 sequence number (volgnummer) is in the same set, only the first can be compared
    Later sequence numbers can only be compared if the previous has been applied first

    :param model: GOB Model for the collection
    :param previous_ids: dictionary with previous volgnummers
    :param event: the event for the data
    :param data:
    :return: data if the data should be recompared after application of the other events, else None
    """
    if model.get("has_states", False):
        entity_id = data['_source_id']
        if previous_ids.get(entity_id):
            previous = previous_ids[entity_id]
            assert previous["volgnummer"] < data["volgnummer"], \
                f'Volgnummer should be sequential {previous["volgnummer"]} !< {data["volgnummer"]}'
            previous_ids[entity_id] = data  # Save this data as last previous data
            return data
        elif event['event'] != 'CONFIRM':
            # Prevent multiple changes in one update
            previous_ids[entity_id] = data


def _dependencies_ok(storage, msg):
    """Check any dependencies for the update of the collection

    :param storage: Storage handler instance for the collection being processed
    :param msg:
    :return: True if all dependencies, if any, are fulfilled
    """
    # Check any dependencies
    depends_on = msg["header"].get("depends_on", {})
    for key, value in depends_on.items():
        # Check every dependency, fail if any is not fulfilled
        if not storage.has_any_entity(key, value):
            return False
    return True


def _compare_new_data(model, storage, new_data=None, entity_id=None):
    """Compare new data with any existing data

    :param model: GOB Model for the collection
    :param storage: Storage handler instance for the collection being processed
    :param new_data:
    :param entity_id: entity if of existing data
    :return:
    """
    assert not (new_data is None and entity_id is None), \
        "One of new data or entity ID should be provided"
    if new_data is None:
        # Deletion
        entity = storage.get_entity_or_none(entity_id)
    else:
        # Add, Confirm, Modify. Get current entity to compare with (None if ADD)
        entity = storage.get_current_entity(new_data)
        # Skip historic volgnummers
        if entity is not None and model.get("has_states", False) is True:
            # Skip any historic states for collections with state
            new_seqnr = new_data["volgnummer"]
            old_seqnr = entity.volgnummer
            if new_seqnr < old_seqnr:
                return
    # calculate modifications, this will be an empty list if either data or entity is empty
    # or if all attributes are equal
    modifications = get_modifications(entity, new_data, model['fields'])
    # construct the event given the entity, data, and metadata
    return get_event_for(entity, new_data, modifications)


def recompare(storage, data):
    """Recompare data with stored data

    :param storage: Storage handler instance for the collection being processed
    :param data:
    :return:
    """
    model = storage.get_collection_model()
    return _compare_new_data(model, storage, new_data=data)
