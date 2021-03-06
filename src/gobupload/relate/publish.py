"""
Relation publication module

Publishes relations as import messages
"""
from gobcore.logging.logger import logger


def publish_result(msg, relates):
    result_msg = {
        'header': msg['header'],
        'summary': logger.get_summary(),
        'contents': relates
    }
    return result_msg
