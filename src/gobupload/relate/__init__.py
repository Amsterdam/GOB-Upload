import datetime

from gobcore.message_broker import publish
from gobcore.logging.logger import logger
from gobcore.model import GOBModel
from gobcore.model.relations import create_relation, DERIVATION, get_relation_name

from gobupload.relate.relate import relate as get_relations
from gobupload.relate.exceptions import RelateException


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

    application = "GOBRelate"
    msg["header"] = {
        **msg.get("header", {}),
        "version": "0.1",
        "source": "GOB",
        "application": application,
        "catalogue": "rel"
    }

    for collection_name in collection_names:
        collection = model.get_collection(catalog_name, collection_name)
        assert collection is not None, f"Invalid collection name '{collection_name}'"
        references = model._extract_references(collection['attributes'])
        for reference_name, reference in references.items():
            relation_name = get_relation_name(model, catalog_name, collection_name, reference_name)
            timestamp = datetime.datetime.utcnow().isoformat()
            process_id = f"{timestamp}.{application}.{catalog_name}.{collection_name}.{reference_name}"
            display_name = f"{catalog_name}:{collection_name} {reference_name}"

            msg["header"].update({
                "entity": relation_name if relation_name else display_name,
                "timestamp": timestamp,
                "process_id": process_id
            })
            logger.configure(msg, "RELATE")
            logger.info(f"Relate {display_name}")

            try:
                relations, src_has_states, dst_has_states = get_relations(
                    catalog_name,
                    collection_name,
                    reference_name
                )
            except RelateException as e:
                logger.error(f"Relate {catalog_name} - {collection_name}:{reference_name} FAILED")
                print(f"Relate Error: {str(e)}")
                continue

            publish_relations(msg, relations, src_has_states, dst_has_states)

            logger.info(f"Relate {catalog_name} - {collection_name}:{reference_name} OK")


def publish_relations(msg, relations, src_has_states, dst_has_states):
    # Convert relations into contents
    contents = []
    has_validity = src_has_states or dst_has_states
    while relations:
        relation = relations.pop(0)

        validity = {
            "begin_geldigheid": relation["begin_geldigheid"],
            "eind_geldigheid": relation["eind_geldigheid"]
        }

        for dst in relation['dst']:
            entity = create_relation(relation['src'], validity, dst, DERIVATION["ON_KEY"])
            if has_validity:
                # Add begin date for uniqueness
                suffix = relation['begin_geldigheid']
                entity['id'] = f"{entity['id']}.{suffix}"
            entity['_source_id'] = entity['id']
            contents.append(entity)

    num_records = len(contents)
    logger.info(f"NUM RECORDS: {num_records}")

    summary = {
        'num_records': num_records
    }

    import_message = {
        "header": msg["header"],
        "summary": summary,
        "contents": contents
    }

    publish("gob.workflow.proposal", "fullimport.proposal", import_message)
