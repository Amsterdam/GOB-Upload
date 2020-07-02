"""
Relate module

Builds relations

Triggers application of the newly found relations on the current entities
Publishes the relation as import messages
"""
import datetime

from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.message_broker.config import CONNECTION_PARAMS, WORKFLOW_EXCHANGE, WORKFLOW_REQUEST_KEY
from gobcore.message_broker.message_broker import Connection as MessageBrokerConnection
from gobcore.model import GOBModel
from gobcore.model.relations import get_relation_name
from gobcore.sources import GOBSources
from gobcore.typesystem import fully_qualified_type_name
from gobcore.typesystem.gob_types import VeryManyReference

from gobupload.relate.update import Relater
from gobupload.relate.publish import publish_result
from gobupload.storage.handler import GOBStorageHandler
from gobupload.storage.materialized_views import MaterializedViews
from gobupload.storage.relate import check_relations, check_very_many_relations, \
                                     check_relation_conflicts

CATALOG_KEY = 'original_catalogue'
COLLECTION_KEY = 'original_collection'
ATTRIBUTE_KEY = 'original_attribute'
RELATE_VERSION = '0.1'


def check_relation(msg):
    """
    Check for any dangling relations

    :param msg:
    :return:
    """
    header = msg.get('header', {})
    catalog_name = header.get('original_catalogue')
    collection_name = header.get('original_collection')
    attribute_name = header.get('original_attribute')

    model = GOBModel()

    logger.configure(msg, "RELATE_CHECK")
    logger.info(f"Relate check started")

    collection = model.get_collection(catalog_name, collection_name)
    assert collection is not None, f"Invalid catalog/collection combination {catalog_name}/{collection_name}"

    reference = model._extract_references(collection['attributes']).get(attribute_name)

    try:
        is_very_many = reference['type'] == fully_qualified_type_name(VeryManyReference)
        check_function = check_very_many_relations if is_very_many else check_relations
        check_function(catalog_name, collection_name, attribute_name)
    except Exception as e:
        _log_exception(f"{attribute_name} check FAILED", e)

    logger.info(f"Relation conflicts check started")
    check_relation_conflicts(catalog_name, collection_name, attribute_name)

    logger.info(f"Relate check completed")

    return {
        "header": msg["header"],
        "summary": {
            'warnings': logger.get_warnings(),
            'errors': logger.get_errors()
        },
        "contents": None
    }


def _split_job(msg: dict):
    header = msg.get('header', {})
    catalog_name = header.get('catalogue')
    collection_name = header.get('collection')
    attribute_name = header.get('attribute')

    assert catalog_name is not None, "A catalog name is required"

    model = GOBModel()
    catalog = model.get_catalog(catalog_name)

    assert catalog is not None, f"Invalid catalog name '{catalog_name}'"

    if collection_name is None:
        collection_names = model.get_collection_names(catalog_name)
    else:
        collection_names = [collection_name]

    assert collection_names, f"No collections specified or found for catalog {catalog_name}"

    with MessageBrokerConnection(CONNECTION_PARAMS) as connection:
        for collection_name in collection_names:
            collection = model.get_collection(catalog_name, collection_name)
            assert collection is not None, f"Invalid collection name '{collection_name}'"

            logger.info(f"** Split {collection_name}")

            attributes = model._extract_references(collection['attributes']) \
                if attribute_name is None \
                else [attribute_name]

            for attr_name in attributes:
                sources = GOBSources()
                relation_specs = sources.get_field_relations(catalog_name, collection_name, attr_name)

                if not relation_specs:
                    logger.info(f"Missing relation specification for {catalog_name} {collection_name} "
                                f"{attr_name}. Skipping")
                    continue

                if relation_specs[0]['type'] == fully_qualified_type_name(VeryManyReference):
                    logger.info(f"Skipping VeryManyReference {catalog_name} {collection_name} {attr_name}")
                    continue

                logger.info(f"Splitting job for {catalog_name} {collection_name} {attr_name}")

                original_header = msg.get('header', {})

                split_msg = {
                    **msg,
                    "header": {
                        **original_header,
                        "catalogue": catalog_name,
                        "collection": collection_name,
                        "attribute": attr_name,
                        "split_from": original_header.get('jobid'),
                    },
                    "workflow": {
                        "workflow_name": "relate",
                    }
                }

                del split_msg['header']['jobid']
                del split_msg['header']['stepid']

                connection.publish(WORKFLOW_EXCHANGE, WORKFLOW_REQUEST_KEY, split_msg)


def prepare_relate(msg):
    """
    The starting point for the relate process. A relate job will be split into individual relate jobs on
    attribute level. If there's only a catalog in the message, all collections of that catalog will be related.
    When a job which has been split is received the relation name will be added and the job will be forwarded
    to the next step of the relate process where the relations are being made.

    :param msg: a message from the broker containing the catalog and collections (optional)
    :return: the result message of the relate preparation step
    """
    header = msg.get('header', {})
    catalog_name = header.get('catalogue')
    collection_name = header.get('collection')
    attribute_name = header.get('attribute')

    application = "GOBRelate"
    msg["header"] = {
        **msg.get("header", {}),
        "version": "0.1",
        "source": "GOB",
        "application": application,
        "entity": collection_name
    }

    timestamp = datetime.datetime.utcnow().isoformat()
    process_id = f"{timestamp}.{application}.{catalog_name}" + \
                 (f".{collection_name}" if collection_name else "") + \
                 (f".{attribute_name}" if attribute_name else "")

    msg["header"].update({
        "timestamp": timestamp,
        "process_id": process_id
    })

    logger.configure(msg, "RELATE")

    if not catalog_name or not collection_name or not attribute_name:
        # A job will be splitted when catalog, collection or attribute are not provided
        logger.info("Splitting relate job")

        _split_job(msg)
        msg['header']['is_split'] = True

        return publish_result(msg, [])
    else:
        # If the job has all attributes, add the relation name and forward to the next step in the relate process
        logger.info(f"** Relate {catalog_name} {collection_name} {attribute_name}")

        relation_name = get_relation_name(GOBModel(), catalog_name, collection_name, attribute_name)

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
    except Exception as e:
        logger.error(str(e))
        raise GOBException(f"Could not get materialized view for relation {relation_name}.")


def _get_materialized_view(catalog_name: str, collection_name: str, attribute_name: str):

    if not collection_name:
        raise GOBException("Need collection_name to update materialized view.")

    if catalog_name == "rel":
        return _get_materialized_view_by_relation_name(collection_name)

    if not attribute_name:
        raise GOBException("Missing attribute")
    try:
        return MaterializedViews().get(catalog_name, collection_name, attribute_name)
    except Exception as e:
        logger.error(str(e))
        raise GOBException(f"Could not get materialized view for {catalog_name} {collection_name}.")


def _check_message(msg: dict):
    required = [CATALOG_KEY, COLLECTION_KEY, ATTRIBUTE_KEY]

    header = msg.get('header', {})

    for key in required:
        if not header.get(key):
            raise GOBException(f"Missing {key} attribute in header")

    model = GOBModel()
    sources = GOBSources()

    if not model.get_catalog(header[CATALOG_KEY]):
        raise GOBException(f"Invalid catalog name {header[CATALOG_KEY]}")

    if not model.get_collection(header[CATALOG_KEY], header[COLLECTION_KEY]):
        raise GOBException(f"Invalid catalog/collection combination: {header[CATALOG_KEY]}/{header[COLLECTION_KEY]}")

    if not sources.get_field_relations(header[CATALOG_KEY], header[COLLECTION_KEY], header[ATTRIBUTE_KEY]):
        raise GOBException(f"Missing relation specification for {header[CATALOG_KEY]} {header[COLLECTION_KEY]} "
                           f"{header[ATTRIBUTE_KEY]}")


def process_relate(msg: dict):
    """
    This function starts the actual relate process. The message is checked for completeness and the Relater
    builds the new or updated relations and returns the result the be compared as if it was the result
    of an import job.

    :param msg: a message from the broker containing the catalog and collections (optional)
    :return: the result message of the relate process
    """
    logger.configure(msg, "RELATE SRC")

    _check_message(msg)
    header = msg.get('header')

    logger.info(f"Relate table started")

    full_update = header.get('mode', "update") == "full"

    if full_update:
        logger.info("Full relate requested")

    updater = Relater(header[CATALOG_KEY], header[COLLECTION_KEY], header[ATTRIBUTE_KEY])

    filename, confirms = updater.update(full_update)

    logger.info(f"Relate table completed")

    relation_name = get_relation_name(GOBModel(), header[CATALOG_KEY], header[COLLECTION_KEY], header[ATTRIBUTE_KEY])

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
        "summary": {
            "warnings": logger.get_warnings(),
            "errors": logger.get_errors(),
        },
        "contents_ref": filename,
        "confirms": confirms,
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

    :param msg:
    :return:
    """
    header = msg.get('header', {})
    catalog_name = header.get('catalogue')
    collection_name = header.get('collection')
    attribute_name = header.get('attribute')

    application = "GOBRelate"

    logger.configure(msg, "UPDATE_VIEW")
    storage_handler = GOBStorageHandler()

    view = _get_materialized_view(catalog_name, collection_name, attribute_name)
    view.refresh(storage_handler)
    logger.info(f"Update materialized view {view.name}")

    timestamp = datetime.datetime.utcnow().isoformat()
    msg['header'].update({
        "timestamp": timestamp,
        "process_id": f"{timestamp}.{application}.{catalog_name}.{collection_name}"
    })

    return msg


def _log_exception(msg, err, MAX_MSG_LENGTH=120):
    """
    Log an exception.

    Use a capped message for the logger
    print the full message on stdout

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
