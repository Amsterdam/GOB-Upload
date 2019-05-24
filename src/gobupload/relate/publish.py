"""
Relation publication module

Publishes relations as import messages
"""
from gobcore.logging.logger import logger
from gobcore.message_broker.offline_contents import ContentsWriter
from gobcore.utils import ProgressTicker
from gobcore.model.relations import create_relation, DERIVATION


def publish_result(msg, relates):
    result_msg = {
        'header': msg['header'],
        'summary': {
            'warnings': logger.get_warnings(),
            'errors': logger.get_errors()
        },
        'contents': relates
    }
    return result_msg


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
    num_records = 0
    has_validity = src_has_states or dst_has_states

    with ContentsWriter() as writer, \
            ProgressTicker(f"Relate", 100) as progress:
        filename = writer.filename
        for relation in relations:
            progress.tick()

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
                writer.write(entity)
                num_records += 1

    if num_records > 0:
        logger.info(f"NUM RECORDS: {num_records}")
    else:
        logger.error("No relations found")

    summary = {
        'num_records': num_records,
        'warnings': logger.get_warnings(),
        'errors': logger.get_errors()
    }

    import_message = {
        "header": msg["header"],
        "summary": summary,
        "contents_ref": filename
    }

    return import_message
