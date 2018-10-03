"""Compare and Upload component

This component reads and writes to the Storage.
It reads the storage to derive events from new uploads
It writes the storage to apply events to the storage

"""
from gobcore.message_broker.config import WORKFLOW_EXCHANGE
from gobcore.message_broker.messagedriven_service import messagedriven_service

from gobuploadservice import compare
from gobuploadservice import update
from gobuploadservice.storage.handler import GOBStorageHandler

SERVICEDEFINITION = {
    'full_import_request': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': 'gob.workflow.request',
        'key': 'fullimport.request',
        'handler': compare.compare,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'queue': 'gob.workflow.proposal',
            'key': 'fullupdate.proposal'
        }
    },
    'full_update_request': {
        'exchange': WORKFLOW_EXCHANGE,
        'queue': 'gob.workflow.request',
        'key': 'fullupdate.request',
        'handler': update.full_update,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'queue': 'gob.workflow.proposal',
            'key': 'updatefinished.proposal'
        }
    },
}

# Initialize database tables
storage = GOBStorageHandler()

messagedriven_service(SERVICEDEFINITION)
