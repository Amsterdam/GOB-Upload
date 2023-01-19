from unittest import TestCase

import json
from gobcore.events import GOB
from gobcore.exceptions import GOBException
from unittest.mock import MagicMock, Mock

from gobupload.storage.handler import GOBStorageHandler
from gobupload.apply.event_applicator import EventApplicator
from tests.fixtures import dict_to_object

from gobupload.update.update_statistics import UpdateStatistics


class TestEventApplicator(TestCase):

    def setUp(self):
        self.stats = MagicMock(spec_set=UpdateStatistics)
        self.storage = MagicMock(spec_set=GOBStorageHandler)
        self.mock_event = {
            "version": "0.1",
            "catalogue": "test_catalogue",
            "application": "TEST",
            "entity": "test_entity",
            "timestamp": None,
            "source": "test",
            "action": "ADD",
            "source_id": "source_id",
            "contents": None,
            "tid": "tid",
        }

    def tearDown(self):
        pass

    def set_contents(self, contents):
        self.mock_event["contents"] = json.dumps(contents)

    def test_constructor(self):
        applicator = EventApplicator(self.storage, set("1"), self.stats)

        assert applicator.inserts == []
        assert applicator.updates == {}
        assert applicator.updates_total == 0
        assert applicator.last_events == set("1")
        assert applicator.add_event_tids == set()

    def test_load(self):
        applicator = EventApplicator(self.storage, set(), self.stats)
        self.mock_event["action"] = "CONFIRM"
        self.set_contents({
            "_tid": "entity_source_id",
            "_hash": "123"
        })
        event = dict_to_object(self.mock_event)

        applicator.load(event)

        assert len(applicator.inserts) == 0
        assert len(applicator.updates) == 1
        assert applicator.updates_total == 1

    def test_apply_new_add(self):
        self.set_contents({"_tid": "entity_source_id", "_hash": "123"})
        event = dict_to_object(self.mock_event)

        with EventApplicator(self.storage, set(), self.stats) as applicator:
            applicator.load(event)

            assert len(applicator.inserts) == 1
            applicator.flush()

        assert len(applicator.inserts) == 0
        self.storage.add_add_events.assert_called()
        self.stats.add_applied.assert_called_with("ADD", 1)

    def test_apply_new_add_exception(self):
        self.set_contents({"_tid": "entity_source_id", "_hash": "123"})
        event = dict_to_object(self.mock_event)

        with self.assertRaises(GOBException):
            with EventApplicator(self.storage, set(), self.stats) as applicator:
                applicator.load(event)

    def test_existing_add(self):
        # Expect add event for existing deleted entity leads to add other event
        self.set_contents({"_hash": "123"})
        event = dict_to_object(self.mock_event)
        event.tid = "existing_source_id"

        applicator = EventApplicator(self.storage, {"existing_source_id"}, self.stats)
        applicator._add_insert = Mock()
        applicator._flush_inserts = Mock()
        applicator._add_update = Mock()

        applicator.load(event)

        applicator._add_insert.assert_not_called()
        applicator._flush_inserts.assert_called_once()
        applicator._add_update.assert_called_once()

    def test_add_update(self):
        event = dict_to_object(self.mock_event.copy())
        event.tid = "any tid"

        event2 = dict_to_object(self.mock_event.copy())
        event2.tid = "any tid2"

        event3 = dict_to_object(self.mock_event.copy())
        event3.tid = "any tid3"

        applicator = EventApplicator(self.storage, set(), self.stats)
        applicator._flush_updates = Mock()

        applicator._add_update(event)
        assert applicator.updates["any tid"] == [event]
        assert applicator.updates_total == 1

        applicator._add_update(event2)
        assert applicator.updates["any tid2"] == [event2]
        assert applicator.updates_total == 2

        applicator._add_update(event3)
        assert applicator.updates["any tid3"] == [event3]
        assert applicator.updates_total == 3

    def test_flush_updates(self):
        event = dict_to_object(self.mock_event.copy())
        event.tid = "any tid"

        applicator = EventApplicator(self.storage, set(), self.stats)
        applicator._update_entity = Mock()
        assert applicator.updates == {}

        applicator._flush_updates()
        assert applicator.updates == {}
        assert applicator.updates_total == 0
        self.storage.get_session.assert_not_called()
        self.storage.get_entities.assert_not_called()

        applicator._add_update(event)
        self.storage.get_entities.return_value = ["any entity"]

        applicator._flush_updates()
        assert applicator.updates == {}
        assert applicator.updates_total == 0

        self.storage.get_entities.assert_called()
        applicator._update_entity.assert_called_with("any entity")

    def test_apply_update(self):
        applicator = EventApplicator(self.storage, set(), self.stats)

        entity = Mock()
        entity._date_deleted = None
        entity._last_event = None
        entity._tid = "any source id"

        gob_event = Mock()
        gob_event.id = "any event id"
        gob_event.tid = "any tid"
        applicator.updates["any source id"] = [gob_event]

        # Normal action, apply event and set last event id
        applicator._update_entity(entity)
        gob_event.apply_to.assert_called_with(entity)
        assert entity._last_event == gob_event.id

        # Apply NON-ADD event on a deleted entity
        applicator.updates["any source id"] = [gob_event]
        entity._date_deleted = "any date deleted"
        with self.assertRaises(GOBException):
            applicator._update_entity(entity)

        # Apply ADD event on a deleted entity is OK
        gob_event = MagicMock(spec=GOB.ADD)
        gob_event.id = "any event id"
        entity._last_event = None
        applicator.updates["any source id"] = [gob_event]
        applicator._update_entity(entity)
        self.assertEqual(entity._last_event, gob_event.id)

        # Apply ADD event on a non-deleted entity raises
        gob_event = MagicMock(spec=GOB.ADD)
        gob_event.id = "any event id"
        gob_event.tid = "any_tid"
        applicator.updates["any source id"] = [gob_event]
        entity._date_deleted = None
        with self.assertRaises(GOBException):
            applicator._update_entity(entity)

    def test_flush(self):
        applicator = EventApplicator(self.storage, set(), self.stats)
        applicator._flush_inserts = Mock()
        applicator._flush_updates = Mock()
        applicator.flush()

        applicator._flush_inserts.assert_called_once()
        applicator._flush_updates.assert_called_once()

    def test_apply_event_batch_add_delete(self):
        """
        Test if a batch of events with ADD -> DELETE -> ADD of the same entity is handled correctly.
        We expect the second ADD event to be handled as an 'other' event, because it needs to revive the deleted
        entity.
        """
        applicator = EventApplicator(self.storage, set(), self.stats)

        test_events = [
            {"action": "ADD", "contents": {"_tid": "any source id"}},
            {"action": "DELETE", "contents": {"_tid": "any source id"}},
            {"action": "ADD", "contents": {"_tid": "any source id"}},
        ]

        for event in test_events:
            self.mock_event['action'] = event['action']
            self.set_contents(event['contents'])
            event_object = dict_to_object(self.mock_event)
            applicator.load(event_object)

        # Expect the first add event to be applied, and a DELETE and ADD event in other events
        # we can't check the parameter, which is removed by self.add_events.clear()
        self.storage.add_add_events.assert_called_once()
        assert len(applicator.inserts) == 0
        assert applicator.updates["tid"][0].action == "DELETE"
        assert applicator.updates["tid"][1].action == "ADD"
        assert applicator.updates_total == 2

    def test_apply_event_batch_modifies(self):
        """
        Test if a batch of events multiple MODIFY events of the same entity is handled correctly.
        We expect the all modify events to be applied
        """
        applicator = EventApplicator(self.storage, set(), self.stats)

        test_events = [
            {"action": "MODIFY", "contents": {"_tid": "any source id"}},
            {"action": "MODIFY", "contents": {"_tid": "any source id"}},
            {"action": "MODIFY", "contents": {"_tid": "any source id"}},
        ]

        for event in test_events:
            self.mock_event["action"] = event["action"]
            self.set_contents(event["contents"])
            event_object = dict_to_object(self.mock_event)
            applicator.load(event_object)

        # Expect all 3 modify events to be added to other events
        assert len(applicator.inserts) == 0
        assert applicator.updates_total == 3
        assert all(obj.action == "MODIFY" for obj in applicator.updates["any source id"])
