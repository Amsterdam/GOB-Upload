import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest import TestCase

import logging
from unittest.mock import ANY, MagicMock, patch, call

from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobupload.apply.event_applicator import EventApplicator

from gobupload.apply.main import _should_analyze, apply, apply_confirm_events, \
    apply_events
from gobupload.storage.handler import GOBStorageHandler
from gobupload.update.update_statistics import UpdateStatistics
from tests import fixtures


class MockCombination:

    def __init__(self, source, catalogue, entity):
        self.source = source
        self.catalogue = catalogue
        self.entity = entity


@patch('gobupload.apply.main.GOBStorageHandler')
class TestApply(TestCase):
    def setUp(self):
        # Disable logging to prevent test from connecting to RabbitMQ
        logging.disable(logging.CRITICAL)
        logger.configure({}, "TEST_APPLY")

        self.mock_storage = MagicMock(spec=GOBStorageHandler)
        self.stats = MagicMock(spec=UpdateStatistics)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    @patch("gobupload.apply.main.EventApplicator", autospec=EventApplicator)
    @patch('gobupload.apply.main.logger', MagicMock())
    def test_apply_events(self, mock_applicator, _):
        event = fixtures.get_event_fixure()
        event.eventid = 100
        event.contents = '{"_entity_source_id": "{fixtures.random_string()}", "entity": {}}'
        self.mock_storage.get_events_starting_after.side_effect = [[event], []]
        stats = MagicMock(spec=UpdateStatistics)

        apply_events(self.mock_storage, set(), 1, stats)

        mock_applicator.assert_called_with(self.mock_storage, set(), stats)
        mock_applicator.return_value.__enter__.return_value.load.assert_called_with(event)
        mock_applicator.return_value.__enter__.return_value.flush.assert_called_once()

        self.mock_storage.get_session.assert_called_once()
        self.mock_storage.get_session.return_value.__enter__.assert_called_once()
        self.mock_storage.get_session.return_value.__exit__.assert_called_once()

        self.mock_storage.get_events_starting_after.has_calls([call(1, 10_000), call(100, 10_000)])

    @patch('gobupload.apply.main.add_notification')
    @patch('gobupload.apply.main.EventNotification')
    @patch('gobupload.apply.main.logger', MagicMock())
    @patch('gobupload.apply.main.apply_events')
    def test_apply_none(self, mock_apply, mock_event_notification, mock_add_notification, mock):
        mock.return_value = self.mock_storage
        self.mock_storage.get_source_catalogue_entity_combinations.return_value = []

        result = apply({'header': {"catalogue": "any_cat"}})

        expected_result_msg = {'header': {"catalogue": "any_cat"}, 'summary': ANY}
        self.assertEqual(result, expected_result_msg)
        mock_apply.assert_not_called()

        # If none are applied, add_notification should not have been called
        mock_add_notification.assert_not_called()

    @patch('gobupload.apply.main.add_notification')
    @patch('gobupload.apply.main.EventNotification')
    @patch('gobupload.apply.main.logger', MagicMock())
    @patch('gobupload.apply.main.get_event_ids', lambda s: (1, 2))
    @patch('gobupload.apply.main.apply_events')
    def test_apply(self, mock_apply, mock_event_notification, mock_add_notification, mock):
        mock.return_value = self.mock_storage
        combination = MockCombination("any source", "any catalogue", "any entity")
        self.mock_storage.get_source_catalogue_entity_combinations.return_value = [combination]

        msg = {"header": {"catalogue": "any cat", "entity": "any ent", "source": "any src"}}
        result = apply(msg)

        self.mock_storage.get_source_catalogue_entity_combinations.assert_called_with(
            catalogue="any cat", entity="any ent", source="any src"
        )

        result_msg = {'header': msg["header"], 'summary': ANY}

        self.assertEqual(result, result_msg)
        mock_apply.assert_called()

        mock_event_notification.assert_called_with({}, [1, 1])
        mock_add_notification.assert_called_with(result_msg, mock_event_notification())

        # collection instead of entity
        apply({"header": {"catalogue": "any cat", "collection": "any ent", "source": "any src"}})
        self.mock_storage.get_source_catalogue_entity_combinations.assert_called_with(
            catalogue="any cat", entity="any ent", source="any src"
        )

        # only catalogue
        apply({"header": {"catalogue": "any cat"}})
        self.mock_storage.get_source_catalogue_entity_combinations.assert_called_with(
            catalogue="any cat", entity=None, source=None
        )

        # empty header raises
        with self.assertRaises(GOBException):
            apply({"header": {}})

    def test_apply_confirms_bulkconfirm_event(self, _):
        msg = {"header": {"timestamp": "any timestamp"}}
        item = {"event": "BULKCONFIRM", "data": {"confirms": [{"_tid": "confirm1"}]}}

        with NamedTemporaryFile(mode="w", delete=False) as tmpfile:
            json.dump(item, tmpfile)
            msg["confirms"] = tmpfile.name

        apply_confirm_events(self.mock_storage, MagicMock(), msg)

        self.mock_storage.apply_confirms.assert_called_with(
            [{"_tid": "confirm1"}], timestamp="any timestamp"
        )
        assert not Path(tmpfile.name).exists()
        assert msg.get("confirms") is None

    def test_apply_confirms_confirm_event(self, _):
        msg = {"header": {"timestamp": "any timestamp"}}
        item = {"event": "CONFIRM", "data": {"some key": "any data"}}

        with NamedTemporaryFile(mode="w", delete=False) as tmpfile:
            json.dump(item, tmpfile)
            msg["confirms"] = tmpfile.name

        apply_confirm_events(self.mock_storage, MagicMock(), msg)

        self.mock_storage.apply_confirms.assert_called_with(
            [{"some key": "any data"}], timestamp="any timestamp"
        )
        assert not Path(tmpfile.name).exists()
        assert msg.get("confirms") is None

    def test_apply_confirms_only_confirm_events(self, _):
        """Assert that only (BULK)CONFIRMS are handled."""
        msg = {"header": {"timestamp": "any timestamp"}}
        item = {"event": "some other event"}

        with NamedTemporaryFile(mode="w", delete=False) as tmpfile:
            json.dump(item, tmpfile)
            msg["confirms"] = tmpfile.name

        with self.assertRaises(GOBException):
            apply_confirm_events(self.mock_storage, MagicMock(), msg)

        assert not Path(tmpfile.name).exists()
        assert "confirms" not in msg

    @patch("gobupload.apply.main._apply_confirms")
    def test_apply_confirms_empty(self, mock_apply, _):
        apply_confirm_events(MagicMock(), MagicMock(), {'header': {}})
        mock_apply.assert_not_called()

        apply_confirm_events(MagicMock(), MagicMock(), {'header': {}, "confirms": None})
        mock_apply.assert_not_called()

        apply_confirm_events(MagicMock(), MagicMock(), {'header': {}, "confirms": []})
        mock_apply.assert_not_called()

    @patch('gobupload.apply.main.add_notification', MagicMock())
    @patch('gobupload.apply.main.logger', MagicMock())
    @patch('gobupload.apply.main.get_event_ids', lambda s: (2, 1))
    @patch('gobupload.apply.main.apply_events')
    def test_apply_corrupted(self, mock_apply, mock):
        mock.return_value = self.mock_storage
        combination = MockCombination("any source", "any catalogue", "any entity")
        self.mock_storage.get_source_catalogue_entity_combinations.return_value = [combination]

        result = apply({'header': {"catalogue": "any_cat"}})

        self.assertEqual(result, {'header': {"catalogue": "any_cat"}, 'summary': ANY})
        mock_apply.assert_not_called()

    @patch('gobupload.apply.main.add_notification', MagicMock())
    @patch('gobupload.apply.main.ContentsReader', MagicMock())
    @patch('gobupload.apply.main.logger', MagicMock())
    @patch('gobupload.apply.main.get_event_ids', lambda s: (1, 1))
    @patch('gobupload.apply.main.apply_events')
    def test_apply_up_to_date(self, mock_apply, mock):
        mock.return_value = self.mock_storage
        combination = MockCombination("any source", "any catalogue", "any entity")
        self.mock_storage.get_source_catalogue_entity_combinations.return_value = [combination]

        result = apply({'header': {"catalogue": "any_cat"}})

        self.assertEqual(result, {'header': {"catalogue": "any_cat"}, 'summary': ANY})
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
        msg = {'header': {'mode': 'full', "catalogue": "any_cat"}}
        mock_should_analyze.return_value = True
        apply(msg)
        mock_storage_handler.return_value.analyze_table.assert_called_once()
        mock_storage_handler.reset_mock()

        # Should analyze is True and mode is not full
        msg = {'header': {'mode': 'notfull', "catalogue": "any_cat"}}
        apply(msg)
        mock_storage_handler.return_value.analyze_table.assert_not_called()

        # Should analyze is False and mode is full
        msg = {'header': {'mode': 'full', "catalogue": "any_cat"}}
        mock_should_analyze.return_value = False
        apply(msg)
        mock_storage_handler.return_value.analyze_table.assert_not_called()

    @patch("gobupload.apply.main.add_notification")
    @patch("gobupload.apply.main._get_source_catalog_entity_combinations")
    @patch("gobupload.apply.main.get_event_ids")
    @patch("gobupload.apply.main.EventNotification")
    @patch("gobupload.apply.main.UpdateStatistics")
    @patch("gobupload.apply.main.apply_confirm_events", MagicMock())
    @patch("gobupload.apply.main.apply_events", MagicMock())
    @patch("gobupload.apply.main._should_analyze", lambda *args: False)
    @patch("gobupload.apply.main.is_corrupted", lambda *args: False)
    def test_apply_notification_eventids(self, mock_statistics, mock_notification, mock_get_event_ids,
                                         mock_get_combinations, mock_add_notification, mock_storage_handler):
        """Tests if the correct before and after event ids are passed in the EventNotification

        :param mock_storage_handler:
        :return:
        """
        mock_statistics().applied = 1

        test_cases = [
            # (number_of_result_combinations, (n*2 calls to get_event_ids max), before, after)
            # Each iteration performs 2 calls to get_event_ids. The items in the list are the values that are returned
            # as the max_eventid for each call.
            (1, [None, 10404], 0, 10404),
            (1, [None, None], 0, 0),
            (3, [20, 100, 40, 120, 10, 150], 10, 150),
            (2, [20, None, None, 30, 22, 28], 0, 30),
        ]

        for combinations_cnt, max_eventids, before, after in test_cases:
            mock_get_combinations.return_value = [MagicMock() for _ in range(combinations_cnt)]
            mock_get_event_ids.side_effect = [(i, 99999999) for i in max_eventids]
            apply({'header': {}})
            mock_notification.assert_called_with(1, [before, after])
            mock_add_notification.assert_called_once()
            mock_add_notification.reset_mock()

        # Test that add_notification is not called when suppress_notifications is set
        mock_get_combinations.return_value = [MagicMock()]
        mock_get_event_ids.side_effect = [(0, 100), (1, 99), ]
        apply({'header': {'suppress_notifications': True}})
        mock_add_notification.assert_not_called()

        # Test that add_notifications is not called when no combinations are present and thus nothing happened
        mock_add_notification.reset_mock()
        mock_get_combinations.return_value = []
        apply({'header': {}})
        mock_add_notification.assert_not_called()
