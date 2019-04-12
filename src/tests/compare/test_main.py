from unittest import TestCase, mock


class TestMain(TestCase):
    @mock.patch('gobupload.storage.handler.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.messagedriven_service')
    def test_main_calls_service_with_definition(self, mock_service, mock_storage):
        from gobupload import __main__
        mock_service.assert_called_with(__main__.SERVICEDEFINITION, "Upload", {"stream_contents": True})

    # Mock this, to prevent service from starting when importing __main__
    @mock.patch('gobupload.storage.handler.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.messagedriven_service')
    def test_servicedefenition(self, mock_service, mock_storage):
        from gobupload import __main__

        for key, definition in __main__.SERVICEDEFINITION.items():
            self.assertTrue('queue' in definition)
            if key != 'full_relate_request':
                self.assertTrue('report' in definition)
            self.assertTrue('handler' in definition)
            self.assertTrue(callable(definition['handler']))
