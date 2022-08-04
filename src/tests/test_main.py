import json
import sys
from argparse import Namespace

from gobupload.__main__ import SERVICEDEFINITION, main, run_as_standalone

from unittest import TestCase, mock

from gobupload.storage.handler import GOBStorageHandler


@mock.patch('gobcore.message_broker.notifications.listen_to_notifications', mock.MagicMock())
class TestMain(TestCase):

    @mock.patch('gobupload.__main__.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.MessagedrivenService')
    def test_main_calls_service_with_definition(self, mock_service, mock_storage):
        # No command line arguments
        sys.argv = ['python -m gobupload']
        print(sys.argv)
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
        # No command line arguments
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

    @mock.patch('gobupload.__main__.GOBStorageHandler')
    def test_run_as_standalone_init_storage(self, mock_storage):
        # No command line arguments
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
        storage = GOBStorageHandler()
        with mock.patch.object(storage, "init_storage") as init_storage_mock:
            result_message = run_as_standalone(
                Namespace(
                    handler="apply",
                    xcom_data=xcom_data,
                    materialized_views=False,
                    mv_name=None
                ),
                storage
            )
            init_storage_mock.assert_called_with()
            assert result_message["header"]["catalogue"] == "catalogue"
            assert result_message["contents_ref"] == "path/to/contents.json"
            assert result_message["notification"]["type"] == "events"

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
