"""Relate module.

Builds relations.

Triggers application of the newly found relations on the current entities.
Publishes the relation as import messages.
"""

import datetime

from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.message_broker.config import CONNECTION_PARAMS, WORKFLOW_EXCHANGE, WORKFLOW_REQUEST_KEY
from gobcore.message_broker.message_broker import Connection as MessageBrokerConnection
from gobcore.model.relations import get_relation_name
from gobcore.sources import GOBSources
from gobcore.typesystem import fully_qualified_type_name
from gobcore.typesystem.gob_types import VeryManyReference

from gobupload import gob_model
from gobupload.storage.handler import GOBStorageHandler
from gobupload.storage.materialized_views import MaterializedViews
from gobupload.storage.relate import check_relations, check_very_many_relations, \
    check_relation_conflicts

from gobupload.relate.update import Relater
from gobupload.relate.publish import publish_result

CATALOG_KEY = 'original_catalogue'
COLLECTION_KEY = 'original_collection'
ATTRIBUTE_KEY = 'original_attribute'
RELATE_VERSION = '0.1'


def get_catalog_from_msg(msg: dict, catalog_key: str):  # noqa: C901
    """Return valid GOBModel catalog (name, dict) tuple.

    :param msg: a message from the broker containing the catalog
    :return: tuple with valid catalog name and catalog dict
    """
    try:
        header = msg['header']
    except KeyError as exc:
        error_msg = "Invalid message: header key is missing"
        _log_exception(error_msg, exc)
        raise GOBException(error_msg) from exc

    try:
        catalog_name = header[catalog_key]
    except KeyError as exc:
        error_msg = "Missing required '{catalog_key}' attribute in message header"
        _log_exception(error_msg, exc)
        raise GOBException(error_msg) from exc

    try:
        catalog = gob_model[catalog_name]
    except KeyError as exc:
        error_msg = f"Invalid catalog name '{catalog_name}'"
        _log_exception(error_msg, exc)
        raise GOBException(error_msg) from exc

    return catalog_name, catalog


def verify_relate_message(msg: dict):
    """Verify relate message.

    Check if the required attributes are present in the message header and
    if header[CATALOG_KEY] is a valid GOBModel catalog.

    :param msg: a message from the broker containing the catalog and collections
    :return:
    """
    _ = get_catalog_from_msg(msg, CATALOG_KEY)
    header = msg['header']

    for key in [COLLECTION_KEY, ATTRIBUTE_KEY]:
        try:
            header[key]
        except KeyError as exc:
            error_msg = f"Missing required {key} attribute in process message header"
            _log_exception(error_msg, exc)
            raise GOBException(error_msg) from exc


def get_collection_from_msg(msg: dict):
    """Return valid GOBModel catalog collection (name, dict) tuple.

    :param msg: a message from the broker containing the catalog and collections
    :return: tuple with valid collection name and collection dict
    """
    verify_relate_message(msg)

    catalog_name = msg['header'][CATALOG_KEY]
    collection_name = msg['header'][COLLECTION_KEY]
    try:
        collection = gob_model[catalog_name]['collections'][collection_name]
    except KeyError as exc:
        error_msg = f"Invalid collection '{collection_name}' for catalog {catalog_name}"
        _log_exception(error_msg, exc)
        raise GOBException(error_msg) from exc

    return collection_name, collection


def check_relation(msg: dict):
    """Check for any dangling relations.

    :param msg: a message from the broker containing the catalog and collections
    :return:
    """
    collection_name, collection = get_collection_from_msg(msg)
    catalog_name = msg['header'][CATALOG_KEY]
    attribute_name = msg['header'][ATTRIBUTE_KEY]
    logger.info(f"Relate check started for {catalog_name}/{collection_name}/{attribute_name}")

    reference = collection['references'].get(attribute_name)
    try:
        is_very_many = reference['type'] == fully_qualified_type_name(VeryManyReference)
        check_function = check_very_many_relations if is_very_many else check_relations
        check_function(catalog_name, collection_name, attribute_name)
    except Exception as exc:
        _log_exception(f"{attribute_name} check FAILED", exc)

    logger.info("Relation conflicts check started")
    check_relation_conflicts(catalog_name, collection_name, attribute_name)

    logger.info("Relate check completed")

    return {
        "header": msg["header"],
        "summary": logger.get_summary(),
        "contents": None
    }


def _split_job(msg: dict):      # noqa: C901
    """Split jobs in message."""
    catalog_name, catalog = get_catalog_from_msg(msg, 'catalogue')

    collection_name = msg['header'].get('collection')
    if collection_name is None:
        collection_names = catalog['collections'].keys()
        assert collection_names, f"No collections specified or found for catalog '{catalog_name}'"
    else:
        try:
            catalog['collections'][collection_name]
        except KeyError as exc:
            error_msg = f"Invalid collection name '{collection_name}'"
            _log_exception(error_msg, exc)
            raise GOBException(error_msg) from exc
        collection_names = [collection_name]

    attribute_name = msg['header'].get('attribute')

    with MessageBrokerConnection(CONNECTION_PARAMS) as connection:
        for collection_name in collection_names:
            logger.info(f"** Split {collection_name}")

            collection = catalog['collections'][collection_name]
            attributes = collection['references'] if attribute_name is None else [attribute_name]

            for attr_name in attributes:
                sources = GOBSources(gob_model)
                relation_specs = sources.get_field_relations(catalog_name, collection_name, attr_name)

                if not relation_specs:
                    logger.warning(
                        f"Missing relation specification for {catalog_name} {collection_name} {attr_name}. Skipping"
                    )
                    continue

                if relation_specs[0]['type'] == fully_qualified_type_name(VeryManyReference):
                    logger.info(f"Skipping VeryManyReference {catalog_name} {collection_name} {attr_name}")
                    continue

                logger.info(f"Splitting job for {catalog_name} {collection_name} {attr_name}")

                split_msg = {
                    **msg,
                    "header": {
                        **msg['header'],
                        "catalogue": catalog_name,
                        "collection": collection_name,
                        "attribute": attr_name,
                        "split_from": msg['header'].get('jobid'),
                    },
                    "workflow": {
                        "workflow_name": "relate",
                    }
                }

                del split_msg['header']['jobid']
                del split_msg['header']['stepid']

                connection.publish(WORKFLOW_EXCHANGE, WORKFLOW_REQUEST_KEY, split_msg)


def prepare_relate(msg):
    """The starting point for the relate process.

    A relate job will be split into individual relate jobs on attribute level. If there's only a
    catalog in the message, all collections of that catalog will be related.
    When a job which has been split is received the relation name will be added and the job will be
    forwarded to the next step of the relate process where the relations are being made.

    :param msg: a message from the broker containing the catalog and collections (optional)
    :return: the result message of the relate preparation step
    """
    header = msg.get('header', {})
    catalog_name = header.get('catalogue')
    collection_name = header.get('collection')
    attribute_name = header.get('attribute')

    application = "GOBRelate"
    msg["header"] = {
        **header,
        "version": "0.1",
        "source": "GOB",
        "application": application,
        "entity": collection_name
    }

    timestamp = datetime.datetime.utcnow().isoformat()
    msg["header"].update({"timestamp": timestamp})

    if not catalog_name or not collection_name or not attribute_name:
        # A job will be splitted when catalog, collection or attribute are not provided
        logger.info("Splitting relate job")

        _split_job(msg)
        msg['header']['is_split'] = True

        return publish_result(msg, [])

    # If the job has all attributes, add the relation name and forward to the next step in the relate process
    logger.info(f"** Relate {catalog_name} {collection_name} {attribute_name}")

    relation_name = get_relation_name(gob_model, catalog_name, collection_name, attribute_name)

    msg["header"].update({
        "catalogue": "rel",
        "collection": relation_name,
        "entity": relation_name,
        "original_catalogue": catalog_name,
        "original_collection": collection_name,
        "original_attribute": attribute_name,
    })

    return msg


def _get_materialized_view_by_relation_name(relation_name: str):
    try:
        return MaterializedViews().get_by_relation_name(relation_name)
    except Exception as exc:
        logger.error(str(exc))
        raise GOBException(f"Could not get materialized view for relation {relation_name}.") from exc


def _get_materialized_view(catalog_name: str, collection_name: str, attribute_name: str):
    if not collection_name:
        raise GOBException("Need collection_name to update materialized view.")

    if catalog_name == "rel":
        return _get_materialized_view_by_relation_name(collection_name)

    if not attribute_name:
        raise GOBException("Missing attribute")
    try:
        return MaterializedViews().get(catalog_name, collection_name, attribute_name)
    except Exception as exc:
        logger.error(str(exc))
        raise GOBException(
            f"Could not get materialized view for {catalog_name} {collection_name}.") from exc


def verify_process_message(msg: dict):
    """Verify message for the relate process.

    Check message header and check if catalog and collection are valid.
    Check if the relation specification in GOBSources is valid.

    :param msg: a message from the broker containing the catalog and collections
    :return:
    """
    _ = get_collection_from_msg(msg)
    header = msg['header']

    sources = GOBSources(gob_model)
    if not sources.get_field_relations(
            header[CATALOG_KEY], header[COLLECTION_KEY], header[ATTRIBUTE_KEY]):
        raise GOBException(
            f"Missing relation specification for {header[CATALOG_KEY]} {header[COLLECTION_KEY]}"
            f" {header[ATTRIBUTE_KEY]}")


def process_relate(msg: dict):
    """This function starts the actual relate process.

    The message is checked for completeness and the Relater builds the new or updated relations
    and returns the result the be compared as if it was the result of an import job.

    :param msg: a message from the broker containing the catalog and collections (optional)
    :return: the result message of the relate process
    """
    verify_process_message(msg)
    header = msg['header']

    logger.info("Relate table started")

    full_update = header.get('mode', "update") == "full"

    if full_update:
        logger.info("Full relate requested")

    storage = GOBStorageHandler()
    filename = None

    with (
        storage.get_session(invalidate=True) as session,
        Relater(session, header[CATALOG_KEY], header[COLLECTION_KEY], header[ATTRIBUTE_KEY]) as updater
    ):
        filename = updater.update(full_update)

    logger.info("Relate table completed")

    relation_name = get_relation_name(
        gob_model, header[CATALOG_KEY], header[COLLECTION_KEY], header[ATTRIBUTE_KEY]
    )

    result_msg = {
        "header": {
            **msg["header"],
            "catalogue": "rel",
            "collection": relation_name,
            "entity": relation_name,
            "source": "GOB",
            "application": "GOB",
            "version": RELATE_VERSION,
            "timestamp": msg.get("timestamp", datetime.datetime.utcnow().isoformat()),
        },
        "summary": logger.get_summary(),
        "contents_ref": filename,
    }

    return result_msg


def update_materialized_view(msg):
    """Updates materialized view for a relation for a given catalog, collection and attribute or relation name.

    Expects a message with headers:
    - catalogue
    - collection (if catalogue is 'rel' this should be the relation_name)
    - attribute (optional if catalogue is 'rel')

    examples of correct headers that are functionally equivalent:
    header = {
        "catalogue": "meetbouten",
        "collection": "meetbouten",
        "attribute": "ligt_in_buurt",
    }
    header = {
        "catalogue": "rel",
        "collection": "mbn_mbt_gbd_brt_ligt_in_buurt",
    }

    :param msg: a message from the broker containing the catalog and collections
    :return:
    """
    header = msg.get('header', {})
    catalog_name = header.get('catalogue')
    collection_name = header.get('collection')
    attribute_name = header.get('attribute')

    storage_handler = GOBStorageHandler()

    view = _get_materialized_view(catalog_name, collection_name, attribute_name)
    view.refresh(storage_handler)
    logger.info(f"Update materialized view {view.name}")

    timestamp = datetime.datetime.utcnow().isoformat()
    msg["header"].update({"timestamp": timestamp})

    return msg


def _log_exception(msg, err, MAX_MSG_LENGTH=120):
    """Log an exception.

    Use a capped message for the logger.
    Print the full message on stdout.

    :param msg: What went wrong
    :param err: Exception
    :return: None
    """
    err = str(err)
    msg = f"{msg}: {err}"
    # Log a capped message
    logger.error(msg if len(msg) <= MAX_MSG_LENGTH else f"{msg[:MAX_MSG_LENGTH]}...")
    # Print the full message
    print(msg)
