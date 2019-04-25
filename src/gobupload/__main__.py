"""Compare and Upload component

This component reads and writes to the Storage.
It reads the storage to derive events from new uploads
It writes the storage to apply events to the storage

"""
from gobcore.message_broker.config import WORKFLOW_EXCHANGE, RESULT_QUEUE
from gobcore.message_broker.messagedriven_service import messagedriven_service

from gobupload import compare
from gobupload import relate
from gobupload import update
from gobupload.storage.handler import GOBStorageHandler

SERVICEDEFINITION = {
    'full_import_request': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': 'gob.workflow.request',
        'key': 'compare.start',
        'handler': compare.compare,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'queue': RESULT_QUEUE,
            'key': 'compare.result'
        }
    },
    'full_update_request': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': 'gob.workflow.request',
        'key': 'fullupdate.start',
        'handler': update.full_update,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'queue': RESULT_QUEUE,
            'key': 'fullupdate.result'
        }
    },
    'full_relate_request': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': 'gob.workflow.request',
        'key': 'relate.start',
        'handler': relate.build_relations
    },
}


# Initialize database tables
storage = GOBStorageHandler()
storage.init_storage()

messagedriven_service(SERVICEDEFINITION, "Upload", {"stream_contents": True})
