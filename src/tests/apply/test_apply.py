import logging

from unittest import TestCase
from unittest.mock import MagicMock, patch, ANY

from gobupload.apply.main import apply_events, apply_confirm_events, apply, _should_analyze
from gobupload.storage.handler import GOBStorageHandler
from tests import fixtures


class MockCombination():

    def __init__(self, source, catalogue, entity):
        self.source = source
        self.catalogue = catalogue
        self.entity = entity


@patch('gobupload.apply.main.GOBStorageHandler')
class TestApply(TestCase):
    def setUp(self):
        # Disable logging to prevent test from connecting to RabbitMQ
        logging.disable(logging.CRITICAL)

        self.mock_storage = MagicMock(spec=GOBStorageHandler)
        self.mock_storage.get_entity_or_none.return_value = None

    def tearDown(self):
        logging.disable(logging.NOTSET)

    @patch('gobupload.apply.main.logger', MagicMock())
    def test_apply_events(self, mock):
        event = fixtures.get_event_fixure()
        event.contents = '{"_entity_source_id": "{fixtures.random_string()}", "entity": {}}'
        mock.return_value = self.mock_storage
        self.mock_storage.get_events_starting_after.side_effect = [[event], []]
        stats = MagicMock()

        apply_events(self.mock_storage, {}, 1, stats)

        stats.add_applied.assert_called()

    @patch('gobupload.apply.main.logger', MagicMock())
    @patch('gobupload.apply.main.apply_events')
    def test_apply_none(self, mock_apply, mock):
        mock.return_value = self.mock_storage
        self.mock_storage.get_source_catalogue_entity_combinations.return_value = []

        result = apply({'header': {}})

        self.assertEqual(result, {'header': {}, 'summary': {'errors': ANY, 'warnings': ANY}})
        mock_apply.assert_not_called()

    @patch('gobupload.apply.main.ContentsReader', MagicMock())
    @patch('gobupload.apply.main.logger', MagicMock())
    @patch('gobupload.apply.main.get_event_ids', lambda s: (1, 2))
    @patch('gobupload.apply.main.apply_events')
    def test_apply(self, mock_apply, mock):
        mock.return_value = self.mock_storage
        combination = MockCombination("any source", "any catalogue", "any entity")
        self.mock_storage.get_source_catalogue_entity_combinations.return_value = [combination]

        result = apply({'header': {}})

        self.assertEqual(result, {'header': {}, 'summary': {'errors': ANY, 'warnings': ANY}})
        mock_apply.assert_called()

    @patch('gobupload.apply.main.os')
    @patch('gobupload.apply.main.ContentsReader')
    def test_apply_confirm_events(self, mock_contents_reader, mock_os, mock):
        mock_stats = MagicMock()
        mock_reader = MagicMock()
        mock_contents_reader.return_value = mock_reader

        msg = {
            'header': {
                'timestamp': 'any timestamp'
            },
            'confirms': 'any filename'
        }

        # Bulkconfirm
        items = [
            {
                'event': 'BULKCONFIRM',
                'data': {
                    'confirms': 'any confirms'
                }
            }
        ]

        mock_reader.items.return_value = items
        apply_confirm_events(self.mock_storage, mock_stats, msg)

        self.mock_storage.apply_confirms.assert_called_with('any confirms', 'any timestamp')
        mock_os.remove.assert_called_with('any filename')
        self.assertIsNone(msg.get('confirms'))

        msg = {
            'header': {
                'timestamp': 'any timestamp'
            },
            'confirms': 'any filename'
        }

        # put CONFIRM data in a list
        items = [
            {
                'event': 'CONFIRM',
                'data': {'some key': 'any data'}
            }
        ]

        mock_reader.items.return_value = items
        apply_confirm_events(self.mock_storage, mock_stats, msg)

        self.mock_storage.apply_confirms.assert_called_with([{'some key': 'any data'}], 'any timestamp')

        msg = {
            'header': {
                'timestamp': 'any timestamp'
            },
            'confirms': 'any filename'
        }

        # Assert that only (BULK)CONFIRMS are handled
        items = [
            {
                'event': 'some other event'
            }
        ]
        mock_reader.items.return_value = items
        with self.assertRaises(AssertionError):
            apply_confirm_events(self.mock_storage, mock_stats, msg)

        # Only execute if msg has confirms
        mock_os.remove.reset_mock()
        msg = {
            'header': {}
        }
        apply_confirm_events(self.mock_storage, mock_stats, msg)
        mock_os.remove.assert_not_called()




    @patch('gobupload.apply.main.logger', MagicMock())
    @patch('gobupload.apply.main.get_event_ids', lambda s: (2, 1))
    @patch('gobupload.apply.main.apply_events')
    def test_apply_corrupted(self, mock_apply, mock):
        mock.return_value = self.mock_storage
        combination = MockCombination("any source", "any catalogue", "any entity")
        self.mock_storage.get_source_catalogue_entity_combinations.return_value = [combination]

        result = apply({'header': {}})

        self.assertEqual(result, {'header': {}, 'summary': {'errors': ANY, 'warnings': ANY}})
        mock_apply.assert_not_called()

    @patch('gobupload.apply.main.ContentsReader', MagicMock())
    @patch('gobupload.apply.main.logger', MagicMock())
    @patch('gobupload.apply.main.get_event_ids', lambda s: (1, 1))
    @patch('gobupload.apply.main.apply_events')
    def test_apply_up_to_date(self, mock_apply, mock):
        mock.return_value = self.mock_storage
        combination = MockCombination("any source", "any catalogue", "any entity")
        self.mock_storage.get_source_catalogue_entity_combinations.return_value = [combination]

        result = apply({'header': {}})

        self.assertEqual(result, {'header': {}, 'summary': {'errors': ANY, 'warnings': ANY}})
        mock_apply.assert_not_called()

    def test_should_analyze(self, mock):
        stats = MagicMock()
        stats.get_applied_stats = lambda: {
            'CONFIRM': {
                'relative': 0.2,
                'absolute': 1,
            }
        }

        self.assertTrue(_should_analyze(stats))

        stats.get_applied_stats = lambda: {
            'CONFIRM': {
                'relative': 0,
                'absolute': 0,
            }
        }
        self.assertFalse(_should_analyze(stats))

        stats.get_applied_stats = lambda: {
            'CONFIRM': {
                'relative': 0.8,
                'absolute': 2,
            }
        }

        self.assertFalse(_should_analyze(stats))

    @patch("gobupload.apply.main.UpdateStatistics")
    @patch("gobupload.apply.main._should_analyze")
    @patch("gobupload.apply.main.get_event_ids", lambda x: (1, 1))
    @patch("gobupload.apply.main.is_corrupted", lambda x, y: True)
    def test_apply_trigger_analyze(self, mock_should_analyze, mock_statistics, mock_storage_handler):
        mock_storage_handler.return_value.get_source_catalogue_entity_combinations.return_value = [type('Res', (), {
            'source': 'the source',
            'catalogue': 'the catalogue',
            'entity': 'the entity',
        })]

        # Should analyze is True and mode is full
        msg = {'header': {'mode': 'full'}}
        mock_should_analyze.return_value = True
        apply(msg)
        mock_storage_handler.return_value.analyze_table.assert_called_once()
        mock_storage_handler.reset_mock()

        # Should analyze is True and mode is not full
        msg = {'header': {'mode': 'notfull'}}
        apply(msg)
        mock_storage_handler.return_value.analyze_table.assert_not_called()

        # Should analyze is False and mode is full
        msg = {'header': {'mode': 'full'}}
        mock_should_analyze.return_value = False
        apply(msg)
        mock_storage_handler.return_value.analyze_table.assert_not_called()
