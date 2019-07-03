from unittest import TestCase, mock


class TestMain(TestCase):
    @mock.patch('gobupload.storage.handler.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.MessagedrivenService')
    def test_main_calls_service_with_definition(self, mock_service, mock_storage):
        from gobupload import __main__
        mock_service.assert_called_with(__main__.SERVICEDEFINITION, "Upload", {"stream_contents": True,
                                                                               "thread_per_service": True})
        mock_service.return_value.start.assert_called_with()

    # Mock this, to prevent service from starting when importing __main__
    @mock.patch('gobupload.storage.handler.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.messagedriven_service')
    def test_servicedefenition(self, mock_service, mock_storage):
        from gobupload import __main__

        for key, definition in __main__.SERVICEDEFINITION.items():
            self.assertTrue('queue' in definition)
            self.assertTrue('handler' in definition)
            self.assertTrue(callable(definition['handler']))
