"""Compare and Upload component

This component reads and writes to the Storage.
It reads the storage to derive events from new uploads
It writes the storage to apply events to the storage

"""
import argparse
import json
import sys
from typing import Any, Optional

from gobcore.datastore.xcom_data_store import XComDataStore
from gobcore.message_broker.config import COMPARE_RESULT_KEY, FULLUPDATE_RESULT_KEY, \
    APPLY_RESULT_KEY, \
    RELATE_PREPARE_RESULT_KEY, RELATE_PROCESS_RESULT_KEY, RELATE_CHECK_RESULT_KEY, \
    RELATE_UPDATE_VIEW_RESULT_KEY
from gobcore.message_broker.config import WORKFLOW_EXCHANGE, FULLUPDATE_QUEUE, \
    COMPARE_QUEUE, APPLY_QUEUE, \
    RELATE_PREPARE_QUEUE, RELATE_PROCESS_QUEUE, RELATE_CHECK_QUEUE, \
    RELATE_UPDATE_VIEW_QUEUE
from gobcore.message_broker.messagedriven_service import messagedriven_service

from gobupload import apply
from gobupload import compare
from gobupload import relate
from gobupload import update
from gobupload.config import DEBUG
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


if DEBUG:
    print("WARNING: Debug mode is ON")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m gobupload",
        description="GOB Upload, Compare and Relate"
    )

    # Upload task arguments
    parser.add_argument(
        "handler",
        choices=list(SERVICEDEFINITION.keys()) + ["migrate"],
        help="Which handler to run."
    )
    # XComData, previously this was a RabbitMQ message
    parser.add_argument(
        "--xcom-data",
        default=json.dumps({}),
        help="XComData, a RabbitMQ message, used by the handler."
    )
    # Additional arguments for migrations
    parser.add_argument(
        "--materialized_views",
        action="store_true",
        default=False,
        help="force recreation of materialized views"
    )
    parser.add_argument(
        "--mv_name",
        nargs="?",
        help="The materialized view to update. Use with --materialized-views"
    )
    return parser.parse_args()


def run_as_standalone(
        args: argparse.Namespace,
        storage: GOBStorageHandler
) -> Optional[dict[str, Any]]:
    # Migrate on request only
    if args.handler == "migrate":
        recreate = [args.mv_name] \
            if args.materialized_views and args.mv_name else args.materialized_views
        storage.init_storage(force_migrate=True, recreate_materialized_views=recreate)
        return

    storage.init_storage()
    print(f"Parsing input xcom data: {args.xcom_data}")
    xcom_msg_data = XComDataStore().parse(args.xcom_data)
    handler = SERVICEDEFINITION.get(args.handler)["handler"]
    message = handler(xcom_msg_data)
    print("Handler result", message)
    XComDataStore().write(message)
    return message
    # TODO: raise sys.exit(1) on error


def main():
    # Initialize database tables
    storage = GOBStorageHandler()

    if len(sys.argv) == 1:
        print("No arguments found, wait for messages on the message broker.")
        storage.init_storage()
        params = {
            "stream_contents": True,
            "thread_per_service": True,
            APPLY_QUEUE: {
                "load_message": False
            }
        }
        messagedriven_service(SERVICEDEFINITION, "Upload", params)
    else:
        print("Arguments found, run as standalone")
        args = parse_arguments()
        run_as_standalone(args, storage)



if __name__ == "__main__":
    main()  # pragma: no cover
