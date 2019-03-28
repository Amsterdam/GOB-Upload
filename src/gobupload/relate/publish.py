"""
Relation publication module

Publishes relations as import messages
"""
from gobcore.message_broker import publish
from gobcore.logging.logger import logger

from gobcore.model.relations import create_relation, DERIVATION


def publish_relations(msg, relations, src_has_states, dst_has_states):
    """
    Publish relations as import messages

    :param msg:
    :param relations:
    :param src_has_states:
    :param dst_has_states:
    :return:
    """
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

    return publish("gob.workflow.proposal", "fullimport.proposal", import_message)