"""
GOB Relations

GOB Relations are defined in GOB Model.
The exact nature of a relation depends on the application that delivers the data.
This can be found in GOB Sources

This module uses the information in GOB Model and GOB Sources to get the relation data.
In time this information can change.
"""
import time

from gobcore.logging.logger import logger

from gobupload.storage.relate import update_relations
from gobupload.relate.exceptions import RelateException


def relate_update(catalog_name, collection_name, reference_name):
    """
    Update all relations for the given catalog, collection and field

    :param catalog_name:
    :param collection_name:
    :param reference_name:
    :return: the relations for the given catalog, collection and field
    """
    start = time.time()
    try:
        count = update_relations(catalog_name, collection_name, reference_name)
    except RelateException as e:
        logger.warning(f"{reference_name} FAILED: {str(e)}")
        print(f"Relate Error: {str(e)}")
    else:
        duration = round(time.time() - start, 2)
        logger.info(f"{reference_name} completed ({duration} secs, {count:,} rows updated) ")
