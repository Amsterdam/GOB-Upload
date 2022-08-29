import importlib
import sys
from unittest import TestCase, mock

from gobupload.__main__ import main, SERVICEDEFINITION


@mock.patch('gobcore.message_broker.notifications.listen_to_notifications', mock.MagicMock())
class TestMain(TestCase):

    @mock.patch('gobupload.__main__.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.MessagedrivenService')
    def test_main_calls_service_with_definition(self, mock_service, mock_storage):
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

    @mock.patch('gobupload.__main__.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.MessagedrivenService')
    def test_main_calls_migrate(self, mock_service, mock_storage):
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
        sys.argv = ['python -m gobupload', 'migrate', '--materialized-views']
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
        sys.argv = ['python -m gobupload', 'migrate', '--materialized-views', '--mv-name', 'some_mv_name']
        main()
        mock_service.return_value.start.assert_not_called()
        mock_storage.return_value.init_storage.assert_called_with(
            force_migrate=True,
            recreate_materialized_views=['some_mv_name']
        )

    @mock.patch('gobupload.__main__.GOBStorageHandler')
    @mock.patch('gobupload.__main__.run_as_standalone')
    def test_main_calls_run_as_standalone(self, mock_run_as_standalone, mock_storage):
        # No command line arguments
        sys.argv = [
            'python -m gobupload',
            'apply',
            '--catalogue', 'test_catalogue',
            '--collection', 'test_entity_autoid'
        ]
        main()
        mock_run_as_standalone.assert_called()
