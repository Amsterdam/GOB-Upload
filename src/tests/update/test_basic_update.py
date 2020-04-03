from unittest import TestCase
from unittest.mock import patch

from gobcore.events.import_events import CONFIRM
from gobupload.update.basic_update import store_events, UpdateException

class TestBasicUpdate(TestCase):

    @patch("gobupload.update.basic_update.GOBStorageHandler")
    def test_store_events(self, mock_storage):
        with self.assertRaises(UpdateException):
            store_events(mock_storage, [{'event': CONFIRM.name}])

        events = [{'event': 'no CONFIRM event'}]
        store_events(mock_storage, events)
        mock_storage.add_events.assert_called_with(events)
