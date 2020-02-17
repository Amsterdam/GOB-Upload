import datetime

from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger

from gobcore.model import GOBModel
from gobcore.sources import GOBSources

from gobcore.model.relations import get_relation_name
from gobupload.relate.table.update_table import RelationTableRelater

CATALOG_KEY = 'original_catalogue'
COLLECTION_KEY = 'original_collection'
ATTRIBUTE_KEY = 'original_attribute'
RELATE_VERSION = '0.1'


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


def relate_table_src_message_handler(msg: dict):
    logger.configure(msg, "RELATE SRC")

    _check_message(msg)
    header = msg.get('header')

    updater = RelationTableRelater(header[CATALOG_KEY], header[COLLECTION_KEY], header[ATTRIBUTE_KEY])
    filename, confirms = updater.update()

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
