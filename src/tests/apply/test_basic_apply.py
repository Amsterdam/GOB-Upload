from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.events.import_events import CONFIRM
from gobupload.apply.basic_apply import apply_unhandled_events, apply_events, apply_confirms, ApplyException

class TestBasicApply(TestCase):

    @patch("gobupload.apply.basic_apply.GOBStorageHandler")
    @patch("gobupload.apply.basic_apply.is_corrupted")
    @patch("gobupload.apply.basic_apply.get_event_ids")
    @patch("gobupload.apply.basic_apply.apply_events")
    def test_apply_unhandled_events(self, mock_apply_events, mock_get_event_ids, mock_is_corrupted, mock_storage):
        mock_get_event_ids.return_value = 100, 100
        mock_is_corrupted.return_value = False
        result = apply_unhandled_events(mock_storage)
        self.assertEqual(result, 100)
        mock_apply_events.assert_not_called()

        mock_get_event_ids.side_effect = [(100, 101), (200, 200)]
        result = apply_unhandled_events(mock_storage)
        mock_apply_events.assert_called_with(mock_storage, 100)
        self.assertEqual(result, 200)

        mock_get_event_ids.side_effect = [(101, 100)]
        mock_is_corrupted.return_value = True
        with self.assertRaises(ApplyException):
            apply_unhandled_events(mock_storage)

    @patch("gobupload.apply.basic_apply.GOBStorageHandler")
    @patch("gobupload.apply.basic_apply._get_gob_event")
    def test_apply_events(self, mock_get_gob_event, mock_storage):
        mock_events = [MagicMock()]
        for mock_event in mock_events:
            mock_event.eventid = 101
        mock_entity = MagicMock()
        mock_entity._last_event = 100
        mock_storage.get_events_starting_after.return_value = mock_events
        mock_storage.get_entity_for_update.return_value = mock_entity
        mock_get_gob_event.return_value = MagicMock()
        apply_events(mock_storage, 100)
        self.assertEqual(mock_entity._last_event, 101)

        mock_entity._last_event = 101
        with self.assertRaises(ApplyException):
            apply_events(mock_storage, 100)

    @patch("gobupload.apply.basic_apply.GOBStorageHandler")
    def test_apply_confirms(self, mock_storage):
        events = [
            {
                'event': CONFIRM.name,
                'data': 'any data'
            },
        ]
        apply_confirms(mock_storage, events, 'any timestamp')
        mock_storage.apply_confirms.assert_called_with(['any data'], 'any timestamp')

        events[0]['event'] = 'non CONFIRM event'
        with self.assertRaises(ApplyException):
            apply_confirms(mock_storage, events, 'any timestamp')
