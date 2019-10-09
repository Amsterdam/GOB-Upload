import logging

from unittest import TestCase
from unittest.mock import MagicMock, patch, ANY

from gobupload.apply.main import apply_events, apply
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
        self.mock_storage.get_events_starting_after.return_value = [event]
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