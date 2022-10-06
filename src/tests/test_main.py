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

    @mock.patch("gobupload.__main__.standalone.run_as_standalone", mock.MagicMock(return_value=0))
    @mock.patch('gobupload.__main__.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.MessagedrivenService')
    def test_main_calls_migrate(self, mock_service, mock_storage):
        sys.argv = ['python -m gobupload', 'migrate']

        with self.assertRaisesRegex(SystemExit, "0"):
            main()

        mock_service.return_value.start.assert_not_called()
        mock_storage.return_value.init_storage.assert_called_with(
            True, False, raise_on_error=True
        )

    @mock.patch("gobupload.__main__.standalone.run_as_standalone", mock.MagicMock(return_value=0))
    @mock.patch('gobupload.__main__.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.MessagedrivenService')
    def test_main_calls_migrate_materialized_views(self, mock_service, mock_storage):
        # With migrate command line arguments
        sys.argv = ['python -m gobupload', 'migrate', '--materialized-views']

        with self.assertRaisesRegex(SystemExit, "0"):
            main()

        mock_service.return_value.start.assert_not_called()
        mock_storage.return_value.init_storage.assert_called_with(
            True, True, raise_on_error=True
        )

    @mock.patch("gobupload.__main__.standalone.run_as_standalone", mock.MagicMock(return_value=0))
    @mock.patch('gobupload.__main__.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.MessagedrivenService')
    def test_main_calls_migrate_single_materialized_view(self, mock_service, mock_storage):
        # With migrate command line arguments
        sys.argv = ['python -m gobupload', 'migrate', '--materialized-views', '--mv-name', 'some_mv_name']

        with self.assertRaisesRegex(SystemExit, "0"):
            main()

        mock_service.return_value.start.assert_not_called()
        mock_storage.return_value.init_storage.assert_called_with(
            True, ['some_mv_name'], raise_on_error=True
        )

    @mock.patch('gobupload.__main__.GOBStorageHandler', mock.MagicMock())
    @mock.patch('gobupload.__main__.standalone.run_as_standalone')
    def test_main_calls_run_as_standalone(self, mock_run_as_standalone):
        # No command line arguments
        sys.argv = [
            'python -m gobupload',
            'apply',
            '--catalogue', 'test_catalogue',
            '--collection', 'test_entity_autoid'
        ]
        with self.assertRaisesRegex(SystemExit, "0"):
            main()

        mock_run_as_standalone.assert_called()
