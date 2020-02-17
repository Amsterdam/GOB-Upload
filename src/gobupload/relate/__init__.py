"""
Relate module

Builds relations

Triggers application of the newly found relations on the current entities
Publishes the relation as import messages
"""
import datetime

from gobcore.exceptions import GOBException
from gobcore.model import GOBModel
from gobcore.sources import GOBSources
from gobcore.logging.logger import logger
from gobcore.model.relations import get_relation_name
from gobcore.message_broker.config import CONNECTION_PARAMS, WORKFLOW_EXCHANGE, WORKFLOW_REQUEST_KEY
from gobcore.message_broker.message_broker import Connection as MessageBrokerConnection

from gobupload.storage.handler import GOBStorageHandler
from gobupload.storage.materialized_views import MaterializedViews
from gobupload.storage.relate import get_last_change, check_relations, check_very_many_relations

from gobupload.relate.relate import relate_update
from gobupload.relate.publish import publish_result


def _relation_needs_update(catalog_name, collection_name, reference_name, reference):
    """
    Tells if a relation needs to be updated

    A message is printed on stdout for debug purposes

    :param catalog_name:
    :param collection_name:
    :param reference_name:
    :param reference:
    :return:
    """
    model = GOBModel()
    display_name = f"{catalog_name}:{collection_name} {reference_name}"

    dst_catalog_name, dst_collection_name = reference['ref'].split(':')

    dst_catalog = model.get_catalog(dst_catalog_name)
    if not dst_catalog:
        print(f"{display_name} skipped, destination catalog missing")
        return False

    dst_collection = model.get_collection(dst_catalog_name, dst_collection_name)
    if not dst_collection:
        print(f"{display_name} skipped, destination collection missing")
        return False

    relation_name = get_relation_name(model, catalog_name, collection_name, reference_name)

    last_src_change = get_last_change(catalog_name, collection_name)
    last_dst_change = get_last_change(dst_catalog_name, dst_collection_name)
    last_rel_change = get_last_change("rel", relation_name)

    if last_rel_change > max(last_src_change, last_dst_change):
        print(f"{display_name} skipped, relations already up-to-date")
        return False

    return True


def _process_references(msg, catalog_name, collection_name, references):
    """
    Process references in the given catalog and collection

    Apply the results on the current entities and publish the results as import messages

    :param msg:
    :param catalog_name:
    :param collection_name:
    :param references:
    :return:
    """
    logger.info(f"Start relate {catalog_name} {collection_name}")

    return [{
        'catalogue': catalog_name,
        'entity': collection_name,
        'reference_name': reference_name,
        'reference': reference
    } for reference_name, reference in references.items()]


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

    logger.configure(msg, "RELATE")
    logger.info(f"Relate check started")

    collection = model.get_collection(catalog_name, collection_name)
    assert collection is not None, f"Invalid catalog/collection combination {catalog_name}/{collection_name}"

    reference = model._extract_references(collection['attributes']).get(attribute_name)

    try:
        is_very_many = reference['type'] == "GOB.VeryManyReference"
        check_function = check_very_many_relations if is_very_many else check_relations
        check_function(catalog_name, collection_name, attribute_name)
    except Exception as e:
        _log_exception(f"{attribute_name} check FAILED", e)

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

            if attribute_name is None:
                attributes = model._extract_references(collection['attributes'])
            else:
                attributes = [attribute_name]

            for attr_name in attributes:
                sources = GOBSources()
                relation_specs = sources.get_field_relations(catalog_name, collection_name, attr_name)

                if not relation_specs:
                    logger.info(f"Missing relation specification for {catalog_name} {collection_name} "
                                f"{attr_name}. Skipping")
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


def build_relations(msg):
    """
    Build all relations for a catalog and collections as specified in the message

    If no collections are specified then all collections in the catalog will be processed

    :param msg: a message from the broker containing the catalog and collections (optional)
    :return: None
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
        logger.info("Splitting relate job")

        _split_job(msg)
        msg['header']['is_split'] = True

        return publish_result(msg, [])
    else:
        logger.info(f"** Relate {catalog_name} {collection_name} {attribute_name}")

        try:
            relate_update(catalog_name, collection_name, attribute_name)
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
        except Exception as e:
            _log_exception(f"{attribute_name} update FAILED", e)


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
