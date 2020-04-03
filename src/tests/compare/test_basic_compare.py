from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.model import FIELD
from gobupload.compare.basic_compare import get_events, _get_event

class TestBasicCompare(TestCase):

    @patch("gobupload.compare.basic_compare.GOBStorageHandler")
    @patch("gobupload.compare.basic_compare.get_modifications")
    @patch("gobupload.compare.basic_compare.get_event_for")
    def test_get_event(self, mock_get_event_for, mock_get_modifications, mock_storage):
        entity = {
            FIELD.HASH: 'any hash'
        }
        entity_model = {
            'all_fields': 'any fields'
        }
        mock_current_entity = MagicMock()
        mock_storage.get_current_entity.return_value = mock_current_entity

        result = _get_event(entity, entity_model, mock_storage)
        mock_get_event_for.assert_called_with(mock_current_entity, entity, mock_get_modifications.return_value)
        self.assertEqual(result, mock_get_event_for.return_value)

    @patch("gobupload.compare.basic_compare.GOBStorageHandler")
    @patch("gobupload.compare.basic_compare.GOBModel", MagicMock())
    @patch("gobupload.compare.basic_compare._get_event")
    @patch("gobupload.compare.basic_compare.Enricher")
    @patch("gobupload.compare.basic_compare.Populator")
    def test_get_events(self, mock_populator, mock_enricher, mock_get_event, mock_storage):
        msg = 'any msg'
        mock_get_event.return_value = 'any event'
        result = get_events(msg, mock_storage, [])
        self.assertEqual(result, [])
        mock_enricher.assert_not_called()
        mock_populator.assert_not_called()

        result = get_events(msg, mock_storage, ['some event', 'some other event'])
        self.assertEqual(result, ['any event', 'any event'])
        mock_enricher.assert_called()
        mock_populator.assert_called()
