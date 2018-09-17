from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.events.import_events import ADD, DELETE, CONFIRM, MODIFY

from gobuploadservice.update import full_update
from gobuploadservice.storage.storage_handler import GOBStorageHandler
from tests import fixtures


@patch('gobuploadservice.update.GOBStorageHandler')
class TestUpdate(TestCase):
    def setUp(self):
        self.mock_storage = MagicMock(spec=GOBStorageHandler)

    def test_fullupdate_saves_event(self, mock):
        mock.return_value = self.mock_storage

        message = fixtures.get_event_message_fixture()
        full_update(message)

        self.mock_storage.add_event_to_db.assert_called_with(message['contents'][0])

    @patch('gobuploadservice.update.GobEvent')
    def test_fullupdate_creates_event_and_pops_ids(self, mock_event, mock):
        mock.return_value = self.mock_storage

        gob_event = MagicMock(wrap=ADD)
        gob_event.pop_ids.return_value = '1', '2'
        mock_event.return_value = gob_event

        message = fixtures.get_event_message_fixture()

        full_update(message)

        mock_event.assert_called()
        gob_event.pop_ids.assert_called()

    @patch('gobuploadservice.update.GobEvent')
    def test_fullupdate_applies_events(self, mock_event, mock):
        mock.return_value = self.mock_storage

        for event_to_test in [ADD, DELETE, MODIFY, CONFIRM]:
            gob_event = MagicMock(wrap=event_to_test)
            mock_event.return_value = gob_event

            message = fixtures.get_event_message_fixture(event_to_test.name)
            id_to_pop = message['contents'][0]['data']['_source_id']
            gob_event.pop_ids.return_value = id_to_pop, id_to_pop

            full_update(message)

            mock_event.assert_called()
            gob_event.pop_ids.assert_called()
            self.mock_storage.get_entity_for_update.assert_called_with(id_to_pop, id_to_pop, gob_event)
            gob_event.apply_to.assert_called()
