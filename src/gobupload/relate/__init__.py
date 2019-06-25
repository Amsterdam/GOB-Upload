"""
Relate module

Builds relations

Triggers application of the newly found relations on the current entities
Publishes the relation as import messages
"""
import datetime

from gobcore.model import GOBModel
from gobcore.sources import GOBSources
from gobcore.logging.logger import logger
from gobcore.model.relations import get_relation_name

from gobupload.storage.relate import get_last_change, check_relations

from gobupload.relate.relate import relate as get_relations
from gobupload.relate.publish import publish_relations, publish_result
from gobupload.relate.exceptions import RelateException


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
    catalog_name = msg['header']['src_catalogue']
    collection_name = msg['header']['src_entity']
    reference_name = msg['header']['src_reference_name']

    display_name = f"{catalog_name}:{collection_name} {reference_name}"

    logger.configure(msg, "RELATE")
    logger.info(f"Relate check '{display_name}' started")

    check_relations(catalog_name, collection_name, reference_name)

    logger.info(f"Relate check '{display_name}' completed")

    return {
        "header": msg["header"],
        "summary": {
            'warnings': logger.get_warnings(),
            'errors': logger.get_errors()
        },
        "contents": None
    }


def relate_relation(msg):
    """
    Derive relations for a specific reference field

    :param msg: Message holding the catalog, collection and reference name
    :return: None
    """
    catalog_name = msg['contents']['catalogue']
    collection_name = msg['contents']['entity']
    reference_name = msg['contents']['reference_name']
    reference = msg['contents']['reference']

    model = GOBModel()
    relation_name = get_relation_name(model, catalog_name, collection_name, reference_name)

    timestamp = datetime.datetime.utcnow().isoformat()
    process_id = f"{timestamp}.{catalog_name}.{collection_name}.{reference_name}"

    display_name = f"{catalog_name}:{collection_name} {reference_name}"

    msg["header"].update({
        "src_catalogue": catalog_name,
        "src_entity": collection_name,
        "src_reference_name": reference_name,
        "entity": relation_name if relation_name else display_name,
        "process_id": process_id
    })

    logger.configure(msg, "RELATE")
    logger.info(f"Relate '{display_name}' started")

    sources = GOBSources()
    relation_specs = sources.get_field_relations(catalog_name, collection_name, reference_name)
    if not relation_specs:
        logger.error(f"Relation {reference_name} is not defined")
        return {
            "header": msg["header"],
            "summary": {
                "errors": logger.get_errors(),
                "warnings": logger.get_warnings()
            },
            "contents": None
        }

    if not _relation_needs_update(catalog_name, collection_name, reference_name, reference):
        logger.info(f"Relation {reference_name} is up-to-date")
        # return {
        #     "header": msg["header"],
        #     "summary": {
        #         "errors": logger.get_errors(),
        #         "warnings": logger.get_warnings(),
        #         "up-to-date": True
        #     },
        #     "contents": None
        # }

    relations = []
    src_has_states = False
    dst_has_states = False
    try:
        relations, src_has_states, dst_has_states = get_relations(
            catalog_name,
            collection_name,
            reference_name
        )
    except RelateException as e:
        logger.error(f"Relate {catalog_name} - {collection_name}:{reference_name} FAILED: {str(e)}")
        print(f"Relate Error: {str(e)}")

    logger.info(f"Relate {display_name} completed")

    # Publish results
    return publish_relations(msg, relations, src_has_states, dst_has_states)


def build_relations(msg):
    """
    Build all relations for a catalog and collections as specified in the message

    If no collections are specified then all collections in the catalog will be processed

    :param msg: a message from the broker containing the catalog and collections (optional)
    :return: None
    """

    catalog_name = msg.get('catalogue')
    collection_names = msg.get('collections')

    assert catalog_name is not None, "A catalog name is required"

    model = GOBModel()
    catalog = model.get_catalog(catalog_name)

    assert catalog is not None, f"Invalid catalog name '{catalog_name}'"

    if collection_names is None:
        collection_names = model.get_collection_names(catalog_name)
    else:
        collection_names = collection_names.split(" ")

    assert collection_names, f"No collections specified or found for catalog {catalog_name}"

    application = "GOBRelate"
    msg["header"] = {
        **msg.get("header", {}),
        "version": "0.1",
        "source": "GOB",
        "application": application,
        "catalogue": "rel"
    }

    timestamp = datetime.datetime.utcnow().isoformat()
    process_id = f"{timestamp}.{application}.{catalog_name}"

    msg["header"].update({
        "entity": f"{catalog_name}",
        "timestamp": timestamp,
        "process_id": process_id
    })
    logger.configure(msg, "RELATE")

    relates = []
    for collection_name in collection_names:
        collection = model.get_collection(catalog_name, collection_name)
        assert collection is not None, f"Invalid collection name '{collection_name}'"

        references = model._extract_references(collection['attributes'])
        relates += _process_references(msg, catalog_name, collection_name, references)

    return publish_result(msg, relates)
