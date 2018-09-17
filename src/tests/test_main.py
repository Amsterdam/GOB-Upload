from unittest import TestCase, mock


class TestMain(TestCase):
    @mock.patch('gobcore.message_broker.messagedriven_service.messagedriven_service')
    def test_main_calls_service_with_definition(self, mock_service):
        from gobuploadservice import __main__
        mock_service.assert_called_with(__main__.SERVICEDEFINITION)

    # Mock this, to prevent service from starting when importing __main__
    @mock.patch('gobcore.message_broker.messagedriven_service.messagedriven_service')
    def test_servicedefenition(self, mock_service):
        from gobuploadservice import __main__

        for key, definition in __main__.SERVICEDEFINITION.items():
            self.assertTrue('queue' in definition)
            self.assertTrue('report_back' in definition)
            self.assertTrue('report_queue' in definition)

            self.assertTrue('handler' in definition)
            self.assertTrue(callable(definition['handler']))
