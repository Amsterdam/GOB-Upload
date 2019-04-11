import logging

from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.exceptions import GOBException
from gobcore.events.import_events import ADD, DELETE, CONFIRM, MODIFY

# from gobupload.update import full_update, UpdateStatistics, _get_gob_event, _get_event_ids, _store_events, _apply_events
from gobupload.update import full_update
from gobupload.update.main import full_update, UpdateStatistics, _get_event_ids, _store_events, _apply_events
from gobupload.update.event_applicator import _get_gob_event
from gobupload.storage.handler import GOBStorageHandler
from tests import fixtures


@patch('gobupload.update.main.GOBStorageHandler')
class TestUpdate(TestCase):
    def setUp(self):
        # Disable logging to prevent test from connecting to RabbitMQ
        logging.disable(logging.CRITICAL)

        self.mock_storage = MagicMock(spec=GOBStorageHandler)
        self.mock_storage.get_entity_or_none.return_value = None

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_get_event_ids(self, _):
        mock_storage = MagicMock()
        mock_storage.get_entity_max_eventid = MagicMock(return_value="max")
        mock_storage.get_last_eventid = MagicMock(return_value="last")
        max_id, last_id = _get_event_ids(mock_storage)
        self.assertEqual(max_id, "max")
        self.assertEqual(last_id, "last")

    @patch('gobupload.update.main._get_event_ids')
    def test_fullupdate_saves_event(self, mock_ids, mock):
        self.mock_storage.get_last_events.return_value = {}
        mock.return_value = self.mock_storage
        mock_ids.return_value = 0, 0

        message = fixtures.get_event_message_fixture()
        full_update(message)

        self.mock_storage.add_events.assert_called_with(message['contents']['events'])

    @patch('gobupload.update.event_applicator.GobEvent')
    @patch('gobupload.update.main._get_event_ids')
    def test_fullupdate_creates_event_and_pops_ids(self, mock_ids, mock_event, mock):
        self.mock_storage.get_last_events.return_value = {}
        mock.return_value = self.mock_storage
        mock_ids.return_value = 0, 0

        gob_event = MagicMock(wrap=ADD)
        gob_event.pop_ids.return_value = '1', '2'
        mock_event.return_value = gob_event

        message = fixtures.get_event_message_fixture()

        self.mock_storage.get_events_starting_after.return_value = []

        full_update(message)

        self.mock_storage.add_events.assert_called()
        self.mock_storage.get_events_starting_after.assert_called()

    @patch('gobupload.update.event_applicator.GobEvent')
    @patch('gobupload.update.main._get_event_ids')
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
        self.mock_storage.get_events_starting_after.assert_called()

    @patch('gobupload.update.event_applicator.GobEvent')
    @patch('gobupload.update.main._get_event_ids')
    def test_fullupdate_applies_events(self, mock_ids, mock_event, mock):
        self.mock_storage.get_last_events.return_value = {}
        mock.return_value = self.mock_storage
        mock_ids.return_value = 0, 0

        for event_to_test in [ADD, DELETE, MODIFY, CONFIRM]:
            gob_event = MagicMock(wrap=event_to_test)
            mock_event.return_value = gob_event

            message = fixtures.get_event_message_fixture(event_to_test.name)
            id_to_pop = message['contents']['events'][0]['data']['_source_id']
            gob_event.pop_ids.return_value = id_to_pop, id_to_pop

            self.mock_storage.get_events_starting_after.return_value = []

            full_update(message)

            self.mock_storage.add_events.assert_called()
            self.mock_storage.get_events_starting_after.assert_called()

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

            # setup done, run gob event
            gob_event = _get_gob_event(dummy_event, data)

            # assert action and last_event are as expected
            self.assertEqual(action_expected, gob_event.name)
            self.assertEqual(last_event_expected, gob_event.last_event)

            # Increase last event for next test
            last_event_expected += 1

    def test_gob_event_invalid_action(self, mock_event):
        # setup initial event and data
        dummy_event = fixtures.get_event_fixure()

        for invalid_action in ['FOO', 'BAR', None, 1]:
            dummy_event.action = invalid_action

            # Assert that Exception is thrown when events have invalid actions
            self.assertRaises(GOBException, _get_gob_event, dummy_event, {})

    def test_store_events(self, mock):
        metadata = fixtures.get_metadata_fixture()
        event = fixtures.get_event_fixture(metadata)
        event['data']['_last_event'] = fixtures.random_string()
        event['data']['_entity_source_id'] = event['data']['_source_id']

        self.mock_storage.get_last_events.return_value = {event['data']['_source_id']: event['data']['_last_event']}
        mock.return_value = self.mock_storage
        stats = UpdateStatistics()

        _store_events(self.mock_storage, [event], stats)

    @patch('gobupload.update.main.logger', MagicMock())
    def test_apply_events(self, mock):
        event = fixtures.get_event_fixure()
        event.contents = '{"_entity_source_id": "{fixtures.random_string()}", "entity": {}}'
        mock.return_value = self.mock_storage
        self.mock_storage.get_events_starting_after.return_value = [event]
        stats = MagicMock()

        _apply_events(self.mock_storage, 1, stats)

        stats.add_applied.assert_called()
