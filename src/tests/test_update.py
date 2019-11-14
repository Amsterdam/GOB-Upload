import logging

from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.exceptions import GOBException
from gobcore.events.import_events import ADD, DELETE, CONFIRM, MODIFY

# from gobupload.update import full_update, UpdateStatistics, _get_gob_event, _get_event_ids, _store_events, _apply_events
from gobupload.update import full_update
from gobupload.update.main import full_update, UpdateStatistics, get_event_ids, _store_events
from gobupload.apply.main import apply_events
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
        max_id, last_id = get_event_ids(mock_storage)
        self.assertEqual(max_id, "max")
        self.assertEqual(last_id, "last")

    @patch('gobupload.update.main.ContentsWriter', MagicMock())
    @patch('gobupload.update.main.get_event_ids')
    def test_fullupdate_saves_event(self, mock_ids, mock):
        self.mock_storage.get_last_events.return_value = {}
        mock.return_value = self.mock_storage
        mock_ids.return_value = 0, 0

        message = fixtures.get_event_message_fixture()
        full_update(message)

        # self.mock_storage.add_events.assert_called_with(message['contents'])

    @patch('gobupload.update.main.ContentsWriter', MagicMock())
    @patch('gobupload.update.event_applicator.GobEvent')
    @patch('gobupload.update.main.get_event_ids')
    def test_fullupdate_creates_event_and_pops_ids(self, mock_ids, mock_event, mock):
        self.mock_storage.get_last_events.return_value = {}
        mock.return_value = self.mock_storage
        mock_ids.return_value = 0, 0

        gob_event = MagicMock(wrap=ADD)
        gob_event.pop_ids.return_value = '1', '2'
        mock_event.return_value = gob_event

        message = fixtures.get_event_message_fixture()

        self.mock_storage.get_events_starting_after.return_value = []

        result = full_update(message)

        self.mock_storage.get_events_starting_after.assert_not_called()
        self.assertIsNotNone(result['confirms'], "")

    @patch('gobupload.update.event_applicator.GobEvent')
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

    @patch('gobupload.update.main.ContentsWriter', MagicMock())
    @patch('gobupload.update.event_applicator.GobEvent')
    @patch('gobupload.update.main.get_event_ids')
    def test_fullupdate_applies_events(self, mock_ids, mock_event, mock):
        self.mock_storage.get_last_events.return_value = {}
        mock.return_value = self.mock_storage
        mock_ids.return_value = 0, 0

        for event_to_test in [ADD, DELETE, MODIFY, CONFIRM]:
            gob_event = MagicMock(wrap=event_to_test)
            mock_event.return_value = gob_event

            message = fixtures.get_event_message_fixture(event_to_test.name)
            id_to_pop = message['contents'][0]['data']['_source_id']
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

    @patch('gobupload.update.main.ContentsWriter', MagicMock())
    def test_store_events(self, mock):
        metadata = fixtures.get_metadata_fixture()
        event = fixtures.get_event_fixture(metadata)
        event['data']['_last_event'] = fixtures.random_string()
        event['data']['_entity_source_id'] = event['data']['_source_id']

        last_events = {event['data']['_source_id']: event['data']['_last_event']}
        mock.return_value = self.mock_storage
        stats = UpdateStatistics()

        _store_events(self.mock_storage, last_events, [event], stats)

    @patch('gobupload.update.main.EventCollector')
    @patch('gobupload.update.main.ContentsWriter')
    def test_store_events_with_confirms(self, mock_contents_writer, mock_event_collector, mock):
        mock_with_writer = MagicMock()
        mock_writer = MagicMock()
        mock_writer.filename = 'any filename'
        mock_with_writer = MagicMock()
        mock_with_writer.__enter__.return_value = mock_writer
        mock_contents_writer.return_value = mock_with_writer

        mock_collector = MagicMock()
        mock_with_collector = MagicMock()
        mock_with_collector.__enter__.return_value = mock_collector
        mock_event_collector.return_value = mock_with_collector

        events = [
            {
                'event': 'CONFIRM'
            },
            {
                'event': 'BULKCONFIRM'
            },
            {
                'event': 'some other event'
            }
        ]

        result = _store_events(self.mock_storage, 'some last events', events, MagicMock())
        self.assertEqual(result, 'any filename')

        self.assertEqual(mock_writer.write.call_count, 2)
        mock_writer.write.assert_called_with({'event': 'BULKCONFIRM'})
        self.assertEqual(mock_collector.collect.call_count, 1)
        mock_collector.collect.assert_called_with({'event': 'some other event'})