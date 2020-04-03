from unittest import TestCase
from unittest.mock import patch

from gobcore.events.import_events import CONFIRM
from gobupload.basic_upload.basic_handler import handle_msg

class TestBasicHandler(TestCase):

    @patch("gobupload.basic_upload.basic_handler.ImportMessage")
    @patch("gobupload.basic_upload.basic_handler.get_events")
    @patch("gobupload.basic_upload.basic_handler.store_events")
    @patch("gobupload.basic_upload.basic_handler.apply_unhandled_events")
    @patch("gobupload.basic_upload.basic_handler.apply_events")
    @patch("gobupload.basic_upload.basic_handler.apply_confirms")
    @patch("gobupload.basic_upload.basic_handler.GOBStorageHandler")
    def test_handle_msg(self, mock_storage_handler, mock_apply_confirms, mock_apply_events, mock_apply_unhandled_events, mock_store_events, mock_get_events, mock_import_message):
        mock_storage = mock_storage_handler.return_value

        mock_get_events.return_value = [{'event': CONFIRM.name}]
        handle_msg('any msg')
        mock_storage_handler.assert_called_with(mock_import_message.return_value.metadata)
        mock_apply_confirms.assert_called()
        mock_store_events.assert_not_called()
        mock_apply_events.assert_not_called()

        mock_apply_confirms.reset_mock()

        mock_events = [{'event': CONFIRM.name}, {'event': 'other event'}]
        mock_get_events.return_value = mock_events
        timestamp = mock_import_message.return_value.metadata.timestamp
        handle_msg('any msg')
        mock_apply_confirms.assert_called_with(mock_storage, [mock_events[0]], timestamp)
        mock_store_events.assert_called_with(mock_storage, [mock_events[1]])
        mock_apply_events.assert_called_with(mock_storage, mock_apply_unhandled_events.return_value)
