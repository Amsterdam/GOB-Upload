import sys
import importlib

from unittest import TestCase, mock

from gobupload.__main__ import build_message, argument_parser, main, run_as_standalone, \
    SERVICEDEFINITION


@mock.patch('gobcore.message_broker.notifications.listen_to_notifications', mock.MagicMock())
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

    @mock.patch('gobupload.storage.handler.GOBStorageHandler')
    @mock.patch('gobcore.message_broker.messagedriven_service.MessagedrivenService')
    def test_main_calls_migrate_single_materialized_view(self, mock_service, mock_storage):
        # With migrate command line arguments
        sys.argv = ['python -m gobupload', '--migrate', '--materialized_views', 'some_mv_name']
        from gobupload import __main__
        importlib.reload(__main__)

        mock_service.return_value.start.assert_not_called()
        mock_storage.return_value.init_storage.assert_called_with(
            force_migrate=True,
            recreate_materialized_views=['some_mv_name']
        )

    @mock.patch('gobupload.storage.handler.GOBStorageHandler')
    @mock.patch('gobupload.__main__.run_as_standalone')
    def test_main_calls_run_as_standalone(self, mock_run_as_standalone, mock_storage):
        # No command line arguments
        sys.argv = [
            'python -m gobupload',
            'apply',
            '--catalogue', 'test_catalogue',
            '--entity', 'test_entity_autoid'
        ]
        main()
        mock_run_as_standalone.assert_called()

    @mock.patch('gobupload.__main__.GOBStorageHandler')
    @mock.patch.dict(SERVICEDEFINITION, {'apply': {
        'handler': mock.MagicMock(__name__="apply_mock", return_value={"msg": "data"})}
    })
    def test_run_as_standalone(self, mock_storage):
        # No command line arguments
        ap = argument_parser()
        args = ap.parse_args([
            'apply', '--catalogue', 'test_catalogue', '--entity', 'test_entity_autoid'
        ])
        assert run_as_standalone(args, SERVICEDEFINITION) == {"msg": "data"}

    def test_build_message(self):
        ap = argument_parser()
        args = ap.parse_args([
            'apply', '--catalogue', 'test_catalogue', '--entity', 'test_entity_autoid'
        ])
        message = build_message(args)
        assert message["catalogue"] == "test_catalogue"
        assert message["entity"] == "test_entity_autoid"
        assert message["collection"] == "test_entity_autoid"
