"""Compare and Upload component

This component reads and writes to the Storage.
It reads the storage to derive events from new uploads
It writes the storage to apply events to the storage

"""
from gobcore.message_broker.messagedriven_service import messagedriven_service
from gobuploadservice.compare import compare
from gobuploadservice.update import full_update

SERVICEDEFINITION = {
    'fullimport.request': {
        'queue': "gob.workflow.request",
        'handler': compare,
        'report_back': 'fullupdate.proposal',
        'report_queue': 'gob.workflow.proposal'
    },
    'fullupdate.request': {
        'queue': "gob.workflow.request",
        'handler': full_update,
        'report_back': 'updatefinished.proposal',
        'report_queue': 'gob.workflow.proposal'
    },
}

messagedriven_service(SERVICEDEFINITION)
