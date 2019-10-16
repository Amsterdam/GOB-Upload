"""Compare and Upload component

This component reads and writes to the Storage.
It reads the storage to derive events from new uploads
It writes the storage to apply events to the storage

"""
import argparse

from gobcore.message_broker.config import WORKFLOW_EXCHANGE, FULLUPDATE_QUEUE, COMPARE_QUEUE, RELATE_QUEUE, \
    CHECK_RELATION_QUEUE, APPLY_QUEUE
from gobcore.message_broker.config import COMPARE_RESULT_KEY, FULLUPDATE_RESULT_KEY, RELATE_RESULT_KEY, \
    CHECK_RELATION_RESULT_KEY, APPLY_RESULT_KEY
from gobcore.message_broker.messagedriven_service import MessagedrivenService

from gobupload import compare
from gobupload import relate
from gobupload import update
from gobupload import apply
from gobupload.storage.handler import GOBStorageHandler

SERVICEDEFINITION = {
    'apply': {
        'queue': APPLY_QUEUE,
        'handler': apply.apply,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': APPLY_RESULT_KEY,
        }
    },
    'compare': {
        'queue': COMPARE_QUEUE,
        'handler': compare.compare,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': COMPARE_RESULT_KEY,
        }
    },
    'full_update': {
        'queue': FULLUPDATE_QUEUE,
        'handler': update.full_update,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': FULLUPDATE_RESULT_KEY,
        }
    },
    'relate': {
        'queue': RELATE_QUEUE,
        'handler': relate.build_relations,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': RELATE_RESULT_KEY,
        }
    },
    'check_relation': {
        'queue': CHECK_RELATION_QUEUE,
        'handler': relate.check_relation,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': CHECK_RELATION_RESULT_KEY,
        }
    },
}

parser = argparse.ArgumentParser(
    prog="python -m gobupload",
    description="GOB Upload, Compare and Relate"
)

parser.add_argument('--migrate',
                    action='store_true',
                    default=False,
                    help='migrate the database tables, views and indexes')
args = parser.parse_args()

# Initialize database tables
storage = GOBStorageHandler()

# Migrate on request only
if args.migrate:
    print("Storage migration forced")
    storage.init_storage(force_migrate=True)
else:
    storage.init_storage()
    params = {
        "stream_contents": True,
        "thread_per_service": True,
        APPLY_QUEUE: {
            "load_message": False
        }
    }
    MessagedrivenService(SERVICEDEFINITION, "Upload", params).start()
