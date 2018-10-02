"""Compare and Upload component

This component reads and writes to the Storage.
It reads the storage to derive events from new uploads
It writes the storage to apply events to the storage

"""
from gobcore.message_broker.messagedriven_service import messagedriven_service
from gobuploadservice import compare
from gobuploadservice import update
from gobuploadservice.storage.handler import GOBStorageHandler

SERVICEDEFINITION = {
    'fullimport.request': {
        'queue': "gob.workflow.request",
        'handler': compare.compare,
        'report_back': 'fullupdate.proposal',
        'report_queue': 'gob.workflow.proposal'
    },
    'fullupdate.request': {
        'queue': "gob.workflow.request",
        'handler': update.full_update,
        'report_back': 'updatefinished.proposal',
        'report_queue': 'gob.workflow.proposal'
    },
}

# Initialize database tables
storage = GOBStorageHandler()

messagedriven_service(SERVICEDEFINITION)
