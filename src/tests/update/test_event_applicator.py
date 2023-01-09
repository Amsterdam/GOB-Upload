from unittest import TestCase

import json
from gobcore.events import GOB
from gobcore.exceptions import GOBException
from unittest.mock import MagicMock

from gobupload.storage.handler import GOBStorageHandler
from gobupload.update.event_applicator import EventApplicator
from tests.fixtures import dict_to_object


class TestEventApplicator(TestCase):

    def setUp(self):
        self.storage = MagicMock(spec=GOBStorageHandler)
        self.mock_event = {
            'version': '0.1',
            'catalogue': 'test_catalogue',
            'application': 'TEST',
            'entity': 'test_entity',
            'timestamp': None,
            'source': 'test',
            'action': 'ADD',
            'source_id': 'source_id',
            'contents': None,
            'tid': 'tid',
        }

    def tearDown(self):
        pass

    def set_contents(self, contents):
        self.mock_event["contents"] = json.dumps(contents)

    def test_constructor(self):
        applicator = EventApplicator(self.storage, set("1"))
        self.assertEqual(applicator.add_events, [])
        self.assertDictEqual(applicator.other_events, {})
        self.assertEqual(applicator.other_events_sum, 0)
        self.assertEqual(applicator.last_events, set("1"))
        self.assertEqual(applicator.add_event_tids, set())

    def test_apply(self):
        applicator = EventApplicator(self.storage, set())
        self.mock_event["action"] = 'CONFIRM'
        self.set_contents({
            '_tid': 'entity_source_id',
            '_hash': '123'
        })
        event = dict_to_object(self.mock_event)
        applicator.apply(event)
        self.assertEqual(len(applicator.add_events), 0)
        self.assertEqual(len(applicator.other_events), 1)

    def test_apply_new_add(self):
        self.set_contents({
            '_tid': 'entity_source_id',
            '_hash': '123'
        })
        event = dict_to_object(self.mock_event)
        with EventApplicator(self.storage, set()) as applicator:
            applicator.apply(event)
            self.assertEqual(len(applicator.add_events), 1)
            applicator.apply_all()
        self.assertEqual(len(applicator.add_events), 0)
        self.storage.add_add_events.assert_called()

    def test_apply_new_add_exception(self):
        self.set_contents({
            '_tid': 'entity_source_id',
            '_hash': '123'
        })
        event = dict_to_object(self.mock_event)
        with self.assertRaises(GOBException):
            with EventApplicator(self.storage, set()) as applicator:
                applicator.apply(event)

    def test_apply_existing_add(self):
        # Expect add event for existing deleted entity leads to add other event
        self.set_contents({
            '_hash': '123'
        })
        event = dict_to_object(self.mock_event)
        event.tid = 'existing_source_id'

        applicator = EventApplicator(self.storage, {'existing_source_id'})
        applicator.add_add_event = MagicMock()
        applicator.apply_add_events = MagicMock()
        applicator.add_other_event = MagicMock()
        applicator.apply(event)

        applicator.add_add_event.assert_not_called()
        applicator.apply_add_events.assert_called_once()
        applicator.add_other_event.assert_called_once()

    def test_add_other_event(self):
        applicator = EventApplicator(self.storage, set())

        applicator.apply_other_events = MagicMock()

        applicator.add_other_event('any gob event1',  'any entity source 1')
        self.assertEqual(applicator.other_events['any entity source 1'], ['any gob event1'])
        self.assertEqual(applicator.other_events_sum, 1)

        applicator.add_other_event('any gob event2', 'any entity source 2')
        self.assertEqual(applicator.other_events['any entity source 2'], ['any gob event2'])
        self.assertEqual(applicator.other_events_sum, 2)

        applicator.add_other_event('any gob event3', 'any entity source 3')
        self.assertEqual(applicator.other_events_sum, 3)

    def test_apply_other_events(self):
        applicator = EventApplicator(self.storage, set())
        applicator.apply_other_event = MagicMock()

        self.assertEqual(applicator.other_events, {})

        applicator.apply_other_events()

        self.assertEqual(applicator.other_events, {})
        self.assertEqual(applicator.other_events_sum, 0)
        self.storage.get_session.assert_not_called()
        self.storage.get_entities.assert_not_called()

        applicator.add_other_event('any gob event', 'any entity source id')
        self.storage.get_entities.return_value = ['any entity']

        applicator.apply_other_events()
        self.assertEqual(applicator.other_events, {})
        self.assertEqual(applicator.other_events_sum, 0)
        self.storage.get_session.return_value.__enter__.assert_called()
        self.storage.get_session.return_value.__exit__.assert_called()
        self.storage.get_entities.assert_called()
        applicator.apply_other_event.assert_called_with('any entity')

    def test_apply_other_event(self):
        applicator = EventApplicator(self.storage, set())

        entity = MagicMock()
        entity._date_deleted = None
        entity._last_event = None

        gob_event = MagicMock()
        gob_event.id = 'any event id'

        entity._tid = 'any source id'
        applicator.other_events['any source id'] = [gob_event]

        # Normal action, apply event and set last event id
        applicator.apply_other_event(entity)
        gob_event.apply_to.assert_called_with(entity)
        self.assertEqual(entity._last_event, gob_event.id)

        # Apply NON-ADD event on a deleted entity
        applicator.other_events['any source id'] = [gob_event]
        entity._date_deleted = 'any date deleted'
        with self.assertRaises(GOBException):
            applicator.apply_other_event(entity)

        # Apply ADD event on a deleted entity is OK
        gob_event = MagicMock(spec=GOB.ADD)
        gob_event.id = 'any event id'
        entity._last_event = None
        applicator.other_events['any source id'] = [gob_event]
        applicator.apply_other_event(entity)
        self.assertEqual(entity._last_event, gob_event.id)

        # Do not set last event id for CONFIRM events
        gob_event = MagicMock(spec=GOB.CONFIRM)
        gob_event.id = 'any event id'
        entity._last_event = None
        entity._date_deleted = None
        applicator.other_events['any source id'] = [gob_event]
        applicator.apply_other_event(entity)
        self.assertEqual(entity._last_event, None)

    def test_apply_all(self):
        applicator = EventApplicator(self.storage, set())
        applicator.apply_add_events = MagicMock()
        applicator.apply_other_events = MagicMock()
        applicator.apply_all()

        applicator.apply_add_events.assert_called_once()
        applicator.apply_other_events.assert_called_once()

    def test_apply_event_batch_add_delete(self):
        """
        Test if a batch of events with ADD -> DELETE -> ADD of the same entity is handled correctly.
        We expect the second ADD event to be handled as an 'other' event, because it needs to revive the deleted
        entity.
        """
        applicator = EventApplicator(self.storage, set())

        test_events = [
            {'action': 'ADD', 'contents': {'_tid': 'any source id'}},
            {'action': 'DELETE', 'contents': {'_tid': 'any source id'}},
            {'action': 'ADD', 'contents': {'_tid': 'any source id'}},
        ]

        test_gob_events = []

        for event in test_events:
            self.mock_event['action'] = event['action']
            self.set_contents(event['contents'])
            event_object = dict_to_object(self.mock_event)
            gob_event, *_ = applicator.apply(event_object)
            test_gob_events.append(gob_event)

        # Expect the first add event to be applied, and a DELETE and ADD event in other events
        # we can't check the parameter, which is removed by self.add_events.clear()
        self.storage.add_add_events.assert_called_once()
        self.assertEqual(len(applicator.add_events), 0)

        self.assertEqual(applicator.other_events["tid"], test_gob_events[1:])
        self.assertEqual(applicator.other_events_sum, 2)

    def test_apply_event_batch_modifies(self):
        """
        Test if a batch of events multiple MODIFY events of the same entity is handled correctly.
        We expect the all modify events to be applied
        """
        applicator = EventApplicator(self.storage, set())

        test_events = [
            {'action': 'MODIFY', 'contents': {'_tid': 'any source id'}},
            {'action': 'MODIFY', 'contents': {'_tid': 'any source id'}},
            {'action': 'MODIFY', 'contents': {'_tid': 'any source id'}},
        ]

        test_gob_events = []

        for event in test_events:
            self.mock_event['action'] = event['action']
            self.set_contents(event['contents'])
            event_object = dict_to_object(self.mock_event)
            gob_event, *_ = applicator.apply(event_object)
            test_gob_events.append(gob_event)

        # Expect all 3 modify events to be added to other events
        self.assertEqual(len(applicator.add_events), 0)
        self.assertEqual(sum([len(x) for x in applicator.other_events.values()]), 3)
