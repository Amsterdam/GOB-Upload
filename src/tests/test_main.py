import sys
import importlib

from unittest import TestCase, mock


class TestMain(TestCase):

    @mock.patch('gobupload.storage.handler.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.MessagedrivenService')
    def test_main_calls_service_with_definition(self, mock_service, mock_storage):
        # No command line arguments
        sys.argv = ['python -m gobupload']
        from gobupload import __main__
        importlib.reload(__main__)

        mock_service.assert_called_with(__main__.SERVICEDEFINITION, "Upload",
                                        {
                                            "stream_contents": True,
                                            "thread_per_service": True,
                                            'gob.workflow.apply.queue': {'load_message': False}
                                        })
        mock_service.return_value.start.assert_called_with()
        mock_storage.return_value.init_storage.assert_called_with()

        for key, definition in __main__.SERVICEDEFINITION.items():
            self.assertTrue('queue' in definition)
            self.assertTrue('handler' in definition)
            self.assertTrue(callable(definition['handler']))

    @mock.patch('gobupload.storage.handler.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.MessagedrivenService')
    def test_main_calls_migrate(self, mock_service, mock_storage):
        # With migrate command line arguments
        sys.argv = ['python -m gobupload', '--migrate']
        from gobupload import __main__
        importlib.reload(__main__)

        mock_service.return_value.start.assert_not_called()
        mock_storage.return_value.init_storage.assert_called_with(
            force_migrate=True,
            recreate_materialized_views=False
        )

    @mock.patch('gobupload.storage.handler.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.MessagedrivenService')
    def test_main_calls_migrate_materialized_views(self, mock_service, mock_storage):
        # With migrate command line arguments
        sys.argv = ['python -m gobupload', '--migrate', '--materialized_views']
        from gobupload import __main__
        importlib.reload(__main__)

        mock_service.return_value.start.assert_not_called()
        mock_storage.return_value.init_storage.assert_called_with(
            force_migrate=True,
            recreate_materialized_views=True
        )
