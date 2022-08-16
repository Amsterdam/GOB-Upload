"""Compare and Upload component

This component reads and writes to the Storage.
It reads the storage to derive events from new uploads
It writes the storage to apply events to the storage

"""
import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional, Dict

from gobcore.message_broker.config import COMPARE_RESULT_KEY, FULLUPDATE_RESULT_KEY, \
    APPLY_RESULT_KEY, \
    RELATE_PREPARE_RESULT_KEY, RELATE_PROCESS_RESULT_KEY, RELATE_CHECK_RESULT_KEY, \
    RELATE_UPDATE_VIEW_RESULT_KEY
from gobcore.message_broker.config import WORKFLOW_EXCHANGE, FULLUPDATE_QUEUE, \
    COMPARE_QUEUE, APPLY_QUEUE, \
    RELATE_PREPARE_QUEUE, RELATE_PROCESS_QUEUE, RELATE_CHECK_QUEUE, \
    RELATE_UPDATE_VIEW_QUEUE
from gobcore.message_broker.messagedriven_service import messagedriven_service
from gobcore.message_broker.offline_contents import load_message, offload_message
from gobcore.message_broker.utils import to_json, from_json

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
    # XComData, previously this was a RabbitMQ message
    parser.add_argument(
        "--xcom-write-path",
        default="/airflow/xcom/return.json",
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


def _write_message(message_out: Dict[str, Any], write_path: Path) -> None:
    """Write message data to a file. Ensures parent directories exist.

    :param message_out: Message data to be written
    :param write_path: Path to write message data to. To use airflow's xcom,
        use `/airflow/xcom/return.json` as a path.
    """
    print(f"Writing message data to {write_path}")
    write_path.parent.mkdir(parents=True, exist_ok=True)
    write_path.write_text(json.dumps(message_out))


def run_as_standalone(args: argparse.Namespace) -> Optional[dict[str, Any]]:
    storage = GOBStorageHandler()
    # Migrate on request only
    if args.handler == "migrate":
        recreate = [args.mv_name] \
            if args.materialized_views and args.mv_name else args.materialized_views
        storage.init_storage(force_migrate=True, recreate_materialized_views=recreate)
        return

    storage.init_storage()

    print(f"Parsing incoming message data: {args.xcom_data}")
    # Load offloaded 'contents_ref'-data into message
    message_in, offloaded_filename = load_message(
        msg=json.loads(args.xcom_data),
        converter=from_json,
        params={"stream_contents": False}
    )
    handler = SERVICEDEFINITION.get(args.handler)["handler"]
    message_out = handler(message_in)
    message_out_offloaded = offload_message(
        msg=message_out,
        converter=to_json,
        force_offload=True
    )

    print(f"Writing message data to {args.xcom_write_path}")
    _write_message(message_out_offloaded, Path(args.xcom_write_path))
    return message_out_offloaded


def run_as_message_driven() -> None:
    """Run in message driven mode, listening to a message queue."""
    storage = GOBStorageHandler()
    storage.init_storage()
    params = {
        "stream_contents": True,
        "thread_per_service": True,
        APPLY_QUEUE: {
            "load_message": False
        }
    }
    messagedriven_service(SERVICEDEFINITION, "Upload", params)


def main():
    if len(sys.argv) == 1:
        print("No arguments found, wait for messages on the message broker.")
        run_as_message_driven()
    else:
        print("Arguments found, run as standalone")
        args = parse_arguments()
        run_as_standalone(args)


if __name__ == "__main__":
    main()  # pragma: no cover
