"""Compare and Upload component

This component reads and writes to the Storage.
It reads the storage to derive events from new uploads
It writes the storage to apply events to the storage

"""
import argparse
import sys

from gobcore.message_broker.config import COMPARE_RESULT_KEY, FULLUPDATE_RESULT_KEY, \
    APPLY_RESULT_KEY, \
    RELATE_PREPARE_RESULT_KEY, RELATE_PROCESS_RESULT_KEY, RELATE_CHECK_RESULT_KEY, \
    RELATE_UPDATE_VIEW_RESULT_KEY, RELATE
from gobcore.message_broker.config import WORKFLOW_EXCHANGE, FULLUPDATE_QUEUE, \
 COMPARE_QUEUE, APPLY_QUEUE, \
 RELATE_PREPARE_QUEUE, RELATE_PROCESS_QUEUE, RELATE_CHECK_QUEUE, \
 RELATE_UPDATE_VIEW_QUEUE
from gobcore.message_broker.messagedriven_service import messagedriven_service
from gobcore.message_broker.typing import ServiceDefinition
from gobcore import standalone
from gobupload import apply
from gobupload import compare
from gobupload import relate
from gobupload import update
from gobupload.storage.handler import GOBStorageHandler
from gobupload.config import DEBUG


SERVICEDEFINITION: ServiceDefinition = {
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
        "logger": RELATE,
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
    parser, subparsers = standalone.parent_argument_parser()
    # Apply handler parser (also called upload_model in workflow)
    apply_parser = subparsers.add_parser(
        name="apply",
        description="Apply events, also known as 'update_model'."
    )
    apply_parser.add_argument(
        "--catalogue",
        required=False,
        help="The name of the data catalogue (example: \"meetbouten\")."
    )
    apply_parser.add_argument(
        "--collection",
        required=False,
        help="The name of the data collection (example: \"metingen\")."
    )
    # Relate handler parser
    relate_parser = subparsers.add_parser(
        name="relate_prepare",
        description="Build relations for a catalogue, also called 'relate'."
    )
    relate_parser.add_argument(
        "--catalogue",
        required=True,
        help="The name of the data catalogue (example: \"meetbouten\")."
    )
    relate_parser.add_argument(
        "--collection",
        help="The name of the data collection (example: \"metingen\")."
    )
    relate_parser.add_argument(
        "--attribute",
        help="The name of the attribute containing the relation to relate "
             "(example: \"ligt_in_buurt\")."
    )
    relate_parser.add_argument(
        "--mode",
        required=False,
        help="The mode to use, defaults to update.",
        default="update",
        choices=["update", "full"]
    )

    subparsers.add_parser(
        name="full_update",
    )
    compare_parser = subparsers.add_parser(
        name="compare",
    )
    compare_parser.add_argument(
        "--mode",
        required=False,
        help="The mode to use, defaults to full.",
        default="full",
        choices=["delete", "full"]
    )
    # Parsers for handlers without any extra arguments.
    # These handlers are not 'start_commands'.
    # These arguments expect data from previous arguments to be passed with
    # --message-data='{...}'
    subparsers.add_parser(
        name="relate_check",
    )
    subparsers.add_parser(
        name="relate_process",
    )
    subparsers.add_parser(
        name="relate_update_view",
    )
    subparsers.add_parser(
        name="upload",
    )

    # Migrate faux handler, which migrates the gob-upload database.
    migrate_parser = subparsers.add_parser(
        name="migrate",
    )
    migrate_parser.add_argument(
        "--materialized-views",
        action="store_true",
        default=False,
        help="Force recreation of materialized views."
    )
    migrate_parser.add_argument(
        "--mv-name",
        nargs="?",
        help="The materialized view to update. Use with --materialized-views."
    )
    return parser


if DEBUG:
    # pragma: no cover
    print("WARNING: Debug mode is ON")


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


def run_as_standalone(args: argparse.Namespace) -> None:
    force_migrate = args.handler == "migrate"

    if force_migrate:
        recreate_materialized_views = \
            [args.mv_name] if args.materialized_views and args.mv_name else args.materialized_views
    else:
        recreate_materialized_views = False

    GOBStorageHandler().init_storage(force_migrate, recreate_materialized_views, raise_on_error=True)
    sys.exit(standalone.run_as_standalone(args, SERVICEDEFINITION))


def main():
    if len(sys.argv) == 1:
        print("No arguments found, wait for messages on the message broker.")
        run_as_message_driven()

    else:
        print("Arguments found, run as standalone")
        parser = argument_parser()
        args = parser.parse_args()
        run_as_standalone(args)


if __name__ == "__main__":
    main()  # pragma: no cover
