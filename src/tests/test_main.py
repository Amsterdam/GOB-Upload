import json
import sys
from argparse import Namespace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from gobcore.utils import get_filename

from gobupload.__main__ import SERVICEDEFINITION, main, run_as_standalone


@mock.patch('gobcore.message_broker.notifications.listen_to_notifications', mock.MagicMock())
class TestMain(TestCase):

    @mock.patch('gobupload.__main__.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.MessagedrivenService')
    def test_main_calls_service_with_definition_on_no_args(self, mock_service, mock_storage):
        # No command line arguments
        sys.argv = ['python -m gobupload']
        main()
        mock_service.assert_called_with(SERVICEDEFINITION, "Upload",
                                        {
                                            "stream_contents": True,
                                            "thread_per_service": True,
                                            'gob.workflow.apply.queue': {'load_message': False}
                                        })
        mock_service.return_value.start.assert_called_with()
        mock_storage.return_value.init_storage.assert_called_with()

        for key, definition in SERVICEDEFINITION.items():
            self.assertTrue('queue' in definition)
            self.assertTrue('handler' in definition)
            self.assertTrue(callable(definition['handler']))

    @mock.patch('gobupload.__main__.run_as_standalone')
    def test_main_calls_run_standalone_on_args(self, run_as_standalone):
        msg = {
            "header": {
                "catalogue": "catalogue",
                "collection": "collection",
                "entity": "entity",
                "source": "GOB",
                "application": "GOB",
                "timestamp": "2022-08-04T11:15:11.715107",
            },
            "summary": [],
            "contents_ref": "path/to/contents.json",
            "confirms": "path/to/confirms.json",
        }
        xcom_data = json.dumps(msg)
        sys.argv = ["python -m gobupload", "--xcom-data", xcom_data, "apply"]
        main()
        run_as_standalone.assert_called()

    @mock.patch.dict(SERVICEDEFINITION, {'apply': {'handler': mock.MagicMock(__name__="apply_mock")}})
    @mock.patch('gobupload.__main__.GOBStorageHandler')
    def test_run_as_standalone_init_storage(self, mock_storage):
        msg = {
            "header": {
                "catalogue": "catalogue",
                "collection": "collection",
                "entity": "entity",
                "source": "GOB",
                "application": "GOB",
                "timestamp": "2022-08-04T11:15:11.715107",
            }
        }
        xcom_data = json.dumps(msg)
        SERVICEDEFINITION["apply"]["handler"].return_value = {}
        run_as_standalone(
            Namespace(
                handler="apply",
                xcom_data=xcom_data,
                xcom_write_path="/airflow/xcom/return.json",
                materialized_views=False,
                mv_name=None
            )
        )
        mock_storage.return_value.init_storage.assert_called()

    @mock.patch.dict(SERVICEDEFINITION, {'apply': {'handler': mock.MagicMock(__name__="apply_mock")}})
    @mock.patch('gobupload.__main__.GOBStorageHandler')
    def test_run_as_standalone_writes_message_data(self, mock_storage):
        msg_in = {
            "header": {
                "catalogue": "catalogue",
                "collection": "collection",
                "entity": "entity",
                "timestamp": "2022-08-04T11:15:11.715107",
            },
            "contents_ref": "contents.json",
            "confirms": "confirms.json",
            "notification": {"type": "events"}
        }
        # Mocked message data from apply function
        msg_apply_out = {
            'header': {
                'catalogue': 'catalogue',
                'collection': 'collection',
                'entity': 'entity',
                'timestamp': '2022-08-04T11:20:00.123456'
            },
            'confirms': 'confirms_out.json',
            'notification': {'type': 'events'},
            # This should end up in a file and should be replaced with
            # contents_ref pointing to that file.
            'contents': [{'offloaded': 'data'}],
            'summary': {'warnings': [], 'errors': [], 'log_counts': {}}
        }
        SERVICEDEFINITION["apply"]["handler"].return_value = msg_apply_out
        with TemporaryDirectory() as tmpdir:
            with mock.patch("gobcore.utils.GOB_SHARED_DIR", str(tmpdir)):
                apply_data_path = get_filename(msg_in["contents_ref"], "message_broker")
                Path(apply_data_path).write_text(json.dumps([{"offloaded": "data"}]))
                run_as_standalone(
                    Namespace(
                        handler="apply",
                        xcom_data=json.dumps(msg_in),
                        materialized_views=False,
                        xcom_write_path="/airflow/xcom/return.json",
                        mv_name=None
                    )
                )
                # The message as passed to airflow, with xcom.
                # run_as_standalone replaces contents with contents_ref.
                with Path("/airflow/xcom/return.json").open() as fp:
                    xcom_data = json.load(fp)
                assert xcom_data["header"]["catalogue"] == "catalogue"
                # Offloading should happen, even if file size is below
                # _MAX_CONTENTS_SIZE
                assert "contents_ref" in xcom_data
                with Path(tmpdir, "message_broker", xcom_data["contents_ref"]).open("r") as fp:
                    apply_contents = json.load(fp)
                    assert apply_contents == [{'offloaded': 'data'}]

    @mock.patch('gobupload.__main__.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.MessagedrivenService')
    def test_main_calls_migrate(self, mock_service, mock_storage):
        # With migrate command line arguments
        sys.argv = ['python -m gobupload', 'migrate']
        main()

        mock_service.return_value.start.assert_not_called()
        mock_storage.return_value.init_storage.assert_called_with(
            force_migrate=True,
            recreate_materialized_views=False
        )

    @mock.patch('gobupload.__main__.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.MessagedrivenService')
    def test_main_calls_migrate_materialized_views(self, mock_service, mock_storage):
        # With migrate command line arguments
        sys.argv = ['python -m gobupload', 'migrate', '--materialized_views']
        main()

        mock_service.return_value.start.assert_not_called()
        mock_storage.return_value.init_storage.assert_called_with(
            force_migrate=True,
            recreate_materialized_views=True
        )

    @mock.patch('gobupload.__main__.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.MessagedrivenService')
    def test_main_calls_migrate_single_materialized_view(self, mock_service, mock_storage):
        # With migrate command line arguments
        sys.argv = ['python -m gobupload', 'migrate', '--materialized_views', '--mv_name', 'some_mv_name']
        main()

        mock_service.return_value.start.assert_not_called()
        mock_storage.return_value.init_storage.assert_called_with(
            force_migrate=True,
            recreate_materialized_views=['some_mv_name']
        )
