import sys

from unittest import TestCase, mock


class TestMain(TestCase):
    @mock.patch('gobupload.storage.handler.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.MessagedrivenService')
    def test_main_calls_service_with_definition(self, mock_service, mock_storage):
        # With command line arguments
        sys.argv = ['python -m gobupload', '--migrate']
        from gobupload import __main__
        mock_service.assert_called_with(__main__.SERVICEDEFINITION, "Upload", {"stream_contents": True,
                                                                               "thread_per_service": True})
        mock_service.return_value.start.assert_called_with()
        mock_storage.return_value.init_storage.assert_called()

    # Mock this, to prevent service from starting when importing __main__
    @mock.patch('gobupload.storage.handler.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.messagedriven_service')
    def test_servicedefenition(self, mock_service, mock_storage):
        # No command line arguments
        sys.argv = ['python -m gobupload']
        from gobupload import __main__

        mock_storage.return_value.init_storage.assert_not_called()
        for key, definition in __main__.SERVICEDEFINITION.items():
            self.assertTrue('queue' in definition)
            self.assertTrue('handler' in definition)
            self.assertTrue(callable(definition['handler']))
