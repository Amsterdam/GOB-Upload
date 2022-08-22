"""Compare and Upload component

This component reads and writes to the Storage.
It reads the storage to derive events from new uploads
It writes the storage to apply events to the storage

"""
import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional, Dict, List, Callable

from gobcore.standalone import default_parser, write_message
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
from gobcore.workflow.start_commands import WorkflowCommands

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


def parse_extra_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Extend default parser with gob-upload specific arguments."""

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
    return parser


def run_as_standalone(args: argparse.Namespace, handler: Callable, message_data: Dict) -> Optional[dict[str, Any]]:
    # Load offloaded 'contents_ref'-data into message
    message_in, offloaded_filename = load_message(
        # msg=json.loads(args.message_data),
        msg=message_data,
        converter=from_json,
        params={"stream_contents": False}
    )
    message_out: Dict[str, Any] = handler(message_in)
    message_out_offloaded = offload_message(
        msg=message_out,
        converter=to_json,
        force_offload=True
    )

    print(f"Writing message data to {args.message_write_path}")
    write_message(message_out_offloaded, Path(args.message_write_path))
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


def migrate_handler(args) -> Dict[str, Any]:
    """Special handler to handle migrate case.

    :return: handlers always return a message, in this case an empty one.
    """
    storage = GOBStorageHandler()
    # Migrate on request only
    recreate = [args.mv_name] \
        if args.materialized_views and args.mv_name else args.materialized_views
    storage.init_storage(force_migrate=True, recreate_materialized_views=recreate)
    return {}


def _get_handler(args: argparse.Namespace) -> Callable:
    if args.handler == "migrate":
        return migrate_handler

    storage = GOBStorageHandler()
    storage.init_storage()
    return SERVICEDEFINITION.get(args.handler)["handler"]


def _construct_message(args):
    # TODO: not finished yet, some required fields should be defined here
    msg = {"header": args}
    return msg


def main():
    if len(sys.argv) == 1:
        print("No arguments found, wait for messages on the message broker.")
        run_as_message_driven()
    else:
        parser = default_parser(list(SERVICEDEFINITION.keys()) + ["migrate"])
        default_args = parser.parse_args()
        print("Arguments found, run as standalone")
        if default_args.message_data:
            parser_extra = parse_extra_arguments(parser)
            extra_args = parser_extra.parse_args()
            run_as_standalone(
                args=extra_args,
                handler=_get_handler(extra_args),
                message_data=json.loads(extra_args.message_data)
            )
        else:
            args = WorkflowCommands([default_args.handler]).parse_arguments()
            run_as_standalone(
                args=args,
                handler=_get_handler(args),
                message_data=_construct_message(args)
            )


if __name__ == "__main__":
    main()  # pragma: no cover
