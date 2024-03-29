import logging
from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.events.import_events import ADD, CONFIRM, DELETE, MODIFY
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger

from gobupload.storage.handler import GOBStorageHandler
from gobupload.apply.event_applicator import database_to_gobevent
from gobupload.update.main import UpdateStatistics, _store_events, full_update, get_event_ids
from tests import fixtures


@patch('gobupload.update.main.GOBStorageHandler')
class TestUpdate(TestCase):
    def setUp(self):
        # Disable logging to prevent test from connecting to RabbitMQ
        logging.disable(logging.CRITICAL)
        logger.configure({}, "TEST_UPDATE")

        self.mock_storage = MagicMock(spec=GOBStorageHandler)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_get_event_ids(self, _):
        mock_storage = MagicMock()
        mock_storage.get_entity_max_eventid = MagicMock(return_value="max")
        mock_storage.get_last_eventid = MagicMock(return_value="last")
        max_id, last_id = get_event_ids(mock_storage)
        self.assertEqual(max_id, "max")
        self.assertEqual(last_id, "last")

    @patch('gobupload.update.main.get_event_ids')
    def test_fullupdate_saves_event(self, mock_ids, mock):
        self.mock_storage.get_last_events.return_value = {}
        mock.return_value = self.mock_storage
        mock_ids.return_value = 0, 0

        message = fixtures.get_event_message_fixture('ADD')

        with patch.multiple("gobupload.update.main.EventCollector", _clear=MagicMock()):
            # by mocking _clear we keep the add_events call, but raise a GOBException
            with self.assertRaises(GOBException):
                full_update(message)

        self.mock_storage.add_events.assert_called_with(message['contents'])

    @patch('gobcore.events.GobEvent')
    @patch('gobupload.update.main.get_event_ids')
    def test_fullupdate_creates_event_and_pops_ids(self, mock_ids, mock_event, mock):
        self.mock_storage.get_last_events.return_value = {}
        mock.return_value = self.mock_storage
        mock_ids.return_value = 0, 0

        gob_event = MagicMock(wrap=ADD)
        gob_event.pop_ids.return_value = '1', '2'
        mock_event.return_value = gob_event

        message = fixtures.get_event_message_fixture('ADD')

        self.mock_storage.get_events_starting_after.return_value = []

        result = full_update(message)

        self.mock_storage.get_events_starting_after.assert_not_called()

    @patch('gobcore.events.GobEvent')
    @patch('gobupload.update.main.get_event_ids')
    def test_fullupdate_not_creates_event_and_pops_ids(self, mock_ids, mock_event, mock):
        mock.return_value = self.mock_storage
        mock_ids.return_value = 0, 1

        gob_event = MagicMock(wrap=ADD)
        gob_event.pop_ids.return_value = '1', '2'
        mock_event.return_value = gob_event

        message = fixtures.get_event_message_fixture()

        self.mock_storage.get_events_starting_after.return_value = []

        full_update(message)

        self.mock_storage.add_events.assert_not_called()
        self.mock_storage.get_events_starting_after.assert_not_called()

    @patch('gobcore.events.GobEvent')
    @patch('gobupload.update.main.get_event_ids')
    def test_fullupdate_applies_events(self, mock_ids, mock_event, mock):
        self.mock_storage.get_last_events.return_value = {}
        mock.return_value = self.mock_storage
        mock_ids.return_value = 0, 0

        for event_to_test in [ADD, DELETE, MODIFY, CONFIRM]:
            gob_event = MagicMock(wrap=event_to_test)
            mock_event.return_value = gob_event

            message = fixtures.get_event_message_fixture(event_to_test.name)
            id_to_pop = message['contents'][0]['data']['_tid']
            gob_event.pop_ids.return_value = id_to_pop, id_to_pop

            self.mock_storage.get_events_starting_after.return_value = []

            full_update(message)

            self.mock_storage.add_events.assert_called()
            self.mock_storage.get_events_starting_after.assert_not_called()

    def test_statistics(self, mock):
        stats = UpdateStatistics()
        for t in ['ADD', 'MODIFY', 'DELETE', 'CONFIRM']:
            stats.store_event({'event': t})
            stats.skip_event({'event': t})
            stats.add_applied(t, 1)
        stats.store_event({'event': 'BULKCONFIRM', 'data': {'confirms': [1, 2, 3]}})
        stats.skip_event({'event': 'BULKCONFIRM', 'data': {'confirms': [1, 2, 3]}})

        results = stats.results()

        self.assertEqual(results['Total events'], 10)
        self.assertEqual(results['Single events'], 8)
        self.assertEqual(results['Bulk events'], 2)
        for t in ['ADD', 'MODIFY', 'DELETE']:
            self.assertEqual(results[f'{t} events stored'], 1)
            self.assertEqual(results[f'{t} events skipped'], 1)
            self.assertEqual(results[f'{t} events applied'], 1)
        self.assertEqual(results[f'CONFIRM events stored'], 4)
        self.assertEqual(results[f'CONFIRM events skipped'], 4)
        self.assertEqual(results[f'CONFIRM events applied'], 1)

    def test_gob_event_action(self, mock_event):
        # setup initial event and data
        dummy_event = fixtures.get_event_fixure()

        last_event_expected = 1
        for action_expected in ['ADD', 'DELETE', 'CONFIRM', 'MODIFY']:
            data = {'_last_event': last_event_expected}
            dummy_event.action = action_expected
            dummy_event.contents = data

            # setup done, run gob event
            gob_event = database_to_gobevent(dummy_event)

            # assert action and last_event are as expected
            self.assertEqual(action_expected, gob_event.name)
            self.assertEqual(last_event_expected, gob_event.last_event)

            # Increase last event for next test
            last_event_expected += 1

    def test_gob_event_invalid_action(self, mock_event):
        # setup initial event and data
        dummy_event = fixtures.get_event_fixure()
        dummy_event.contents = {}

        for invalid_action in ['FOO', 'BAR', None, 1]:
            dummy_event.action = invalid_action

            # Assert that Exception is thrown when events have invalid actions
            self.assertRaises(GOBException, database_to_gobevent, dummy_event)

    @patch("gobupload.update.main.logger")
    def test_store_events(self, mock_logger, mock_storage):
        metadata = fixtures.get_metadata_fixture()
        event = fixtures.get_event_fixture(metadata)
        event['data']['_last_event'] = fixtures.random_string()

        last_events = {event['data']['_tid']: event['data']['_last_event']}
        mock_storage.return_value = self.mock_storage
        stats = UpdateStatistics()

        _store_events(self.mock_storage, last_events, [event], stats)

        assert stats.num_events == 1
        assert stats.num_single_events == 1
        assert len(stats.stored) == 1

        # invalid event
        event['data']['_last_event'] = {"_tid": 1, "_last_event": 100}
        _store_events(self.mock_storage, last_events, [event], stats)

        mock_logger.warning.assert_called_with(f"Invalid event: {event}")

    @patch("gobupload.update.main.get_event_ids", MagicMock(return_value=(0, 0)))
    @patch("gobupload.update.main.is_corrupted", lambda x, y: True)
    @patch("gobupload.update.main.logger")
    def test_fullupdate_model_inconsistent(self, mock_logger, _):
        message = fixtures.get_event_message_fixture()

        full_update(message)
        mock_logger.error.assert_called_with("Model is inconsistent! data is more recent than events")
