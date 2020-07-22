"""Compare and Upload component

This component reads and writes to the Storage.
It reads the storage to derive events from new uploads
It writes the storage to apply events to the storage

"""
import argparse

from gobcore.message_broker.config import WORKFLOW_EXCHANGE, FULLUPDATE_QUEUE, COMPARE_QUEUE, APPLY_QUEUE, \
    RELATE_PREPARE_QUEUE, RELATE_PROCESS_QUEUE, RELATE_CHECK_QUEUE, RELATE_UPDATE_VIEW_QUEUE
from gobcore.message_broker.config import COMPARE_RESULT_KEY, FULLUPDATE_RESULT_KEY, APPLY_RESULT_KEY, \
    RELATE_PREPARE_RESULT_KEY, RELATE_PROCESS_RESULT_KEY, RELATE_CHECK_RESULT_KEY, RELATE_UPDATE_VIEW_RESULT_KEY
from gobcore.message_broker.messagedriven_service import MessagedrivenService

from gobupload import compare
from gobupload import relate
from gobupload import update
from gobupload import apply
from gobupload.storage.handler import GOBStorageHandler
from gobupload.config import DEBUG

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
    'relate_prepare': {
        'queue': RELATE_PREPARE_QUEUE,
        'handler': relate.prepare_relate,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': RELATE_PREPARE_RESULT_KEY,
        }
    },
    'relate_process': {
        'queue': RELATE_PROCESS_QUEUE,
        'handler': relate.process_relate,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': RELATE_PROCESS_RESULT_KEY,
        }
    },
    'relate_check': {
        'queue': RELATE_CHECK_QUEUE,
        'handler': relate.check_relation,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': RELATE_CHECK_RESULT_KEY,
        }
    },
    'relate_update_view': {
        'queue': RELATE_UPDATE_VIEW_QUEUE,
        'handler': relate.update_materialized_view,
        'report': {
            'exchange': WORKFLOW_EXCHANGE,
            'key': RELATE_UPDATE_VIEW_RESULT_KEY
        }
    }
}

parser = argparse.ArgumentParser(
    prog="python -m gobupload",
    description="GOB Upload, Compare and Relate"
)

parser.add_argument('--migrate',
                    action='store_true',
                    default=False,
                    help='migrate the database tables, views and indexes')
parser.add_argument('--materialized_views',
                    action='store_true',
                    default=False,
                    help='force recreation of materialized views')
args = parser.parse_args()

if DEBUG:
    print("WARNING: Debug mode is ON")

# Initialize database tables
storage = GOBStorageHandler()

# Migrate on request only
if args.migrate:
    print("Storage migration forced")
    storage.init_storage(force_migrate=True, recreate_materialized_views=args.materialized_views)
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
