"""Compare and Upload component

This component reads and writes to the Storage.
It reads the storage to derive events from new uploads
It writes the storage to apply events to the storage

"""
import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional, Dict, Callable

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
from gobupload.storage.handler import GOBStorageHandler
from gobupload.config import DEBUG


Message = dict[str, Any]

SERVICEDEFINITION = {
    'apply': {  # in workflow this is called update_model
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


def argument_parser():
    parser = argparse.ArgumentParser(
        description='Start standalone GOB Tasks',
    )
    parser.add_argument(
         "--message-data",
         default=json.dumps({}),
         help="Message data used by the handler."
     )
    parser.add_argument(
        "--message-result-path",
        default="/airflow/xcom/return.json",
        help="Path to store result message."
    )
    subparsers = parser.add_subparsers(
        title="subcommands",
        help="Which handler to run.",
        dest="handler",
    )
    # Apply handler parser (also called upload_model in workflow)
    apply_parser = subparsers.add_parser(
        name="apply",
        description="Apply events, also known as 'update_model'."
    )
    apply_parser.add_argument(
        "--catalogue",
        required=False,
        help="The name of the data catalogue (example: \"meetbouten\")"
    )
    apply_parser.add_argument(
        "--entity",
        required=False,
        help="The name of the data collection (example: \"metingen\")"
    )
    # Relate handler parser
    relate_parser = subparsers.add_parser(
        name="relate",
        description="Apply events, also known as 'update_model'."
    )
    relate_parser.add_argument(
        "--catalogue",
        required=True,
        help="The name of the data catalogue (example: \"meetbouten\")"
    )
    relate_parser.add_argument(
        "--collection",
        help="The name of the data collection (example: \"metingen\")"
    )
    relate_parser.add_argument(
        "--attribute",
        help="The name of the attribute containing the relation to relate (example: \"ligt_in_buurt\")"
    )
    relate_parser.add_argument(
        "--mode",
        # named=True,
        required=False,
        help="The mode to use. Defaults to update",
        default="update",
        choices=["update", "full"]
    )
    # Migrate faux handler, which migrates the gob-upload database.
    migrate_parser = subparsers.add_parser(
        name="migrate",
    )
    migrate_parser.add_argument(
        "--migrate",
        action="store_true",
        default=False,
        help="migrate the database tables, views and indexes"
    )
    migrate_parser.add_argument(
        "--materialized_views",
        action="store_true",
        default=False,
        help="force recreation of materialized views"
    )
    migrate_parser.add_argument(
        "--mv_name",
        nargs="?",
        help="The materialized view to update. Use with --materialized-views"
    )
    return parser


if DEBUG:
    print("WARNING: Debug mode is ON")


def _write_message(message_out: Dict[str, Any], write_path: Path) -> None:
    """Write message data to a file. Ensures parent directories exist.
    :param message_out: Message data to be written
    :param write_path: Path to write message data to. To use airflow's xcom,
        use `/airflow/xcom/return.json` as a path.
    """
    print(f"Writing message data to {write_path}")
    write_path.parent.mkdir(parents=True, exist_ok=True)
    write_path.write_text(json.dumps(message_out))


def build_message(args: argparse.Namespace) -> Message:
    """Create a message from argparse arguments.

    Defaults to None if attribute has no value.

    :param args: Parsed arguments
    :return: A message with keys as required by different handlers.
    """
    return {
        'header': {
            'catalogue': getattr(args, "catalogue", None),
            'mode': getattr(args, "mode", None),
            'collection': getattr(args, "collection", None),
            'entity': getattr(args, "collection", None),
            'attribute': getattr(args, "attribute", None)
        }
    }


def get_handler(handler: str, mapping: Dict[str, Any]) -> Callable:
    """Returns handler from a dictionary which is formatted like:

    {
        "handler_name": {
            "handler": some_callable
        }
    }

    This mapping usually is SERVICEDEFINITION.

    :param handler: name of the handler to lookup in the mapping.
    :param mapping: mapping formatted as described above.
    :returns: A callable.
    """
    return mapping.get(handler)["handler"]


def run_as_standalone(
        args: argparse.Namespace, service_definition: dict[str, Any]
) -> Optional[Message]:
    message = build_message(args)
    storage = GOBStorageHandler()
    storage.init_storage()

    print(f"Loading incoming message: {message}")
    # Load offloaded 'contents_ref'-data into message
    message_in, offloaded_filename = load_message(
        msg=message,
        converter=from_json,
        params={"stream_contents": False}
    )
    print("Fully loaded incoming message, including data:")
    print(message_in)
    handler = get_handler(args.handler, service_definition)
    message_out = handler(message_in)
    message_out_offloaded = offload_message(
        msg=message_out,
        converter=to_json,
        force_offload=True
    )

    print(f"Writing message data to {args.message_result_path}")
    _write_message(message_out_offloaded, Path(args.message_result_path))
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
        parser = argument_parser()
        args = parser.parse_args()

        # Special case for migrate, which is specific to upload
        if args.handler == "migrate":
            recreate = [args.mv_name] \
                if args.materialized_views and args.mv_name else args.materialized_views
            storage = GOBStorageHandler()
            storage.init_storage(
                force_migrate=True,
                recreate_materialized_views=recreate
            )
            return

        run_as_standalone(args, SERVICEDEFINITION)


if __name__ == "__main__":
    main()  # pragma: no cover
