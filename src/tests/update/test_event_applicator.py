from unittest import TestCase

import json
from gobcore.events import GOB
from gobcore.exceptions import GOBException
from unittest.mock import MagicMock, patch, ANY

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
            'contents': None
        }

    def tearDown(self):
        pass

    def set_contents(self, contents):
        self.mock_event["contents"] = json.dumps(contents)

    def test_constructor(self):
        applicator = EventApplicator(self.storage, {})
        self.assertEqual(applicator.add_events, [])

    def test_apply(self):
        applicator = EventApplicator(self.storage, {})
        self.mock_event["action"] = 'CONFIRM'
        self.set_contents({
            '_entity_source_id': 'entity_source_id',
            '_hash': '123'
        })
        event = dict_to_object(self.mock_event)
        applicator.apply(event)
        self.assertEqual(len(applicator.add_events), 0)
        self.assertEqual(len(applicator.other_events), 1)

    def test_apply_new_add(self):
        self.set_contents({
            '_entity_source_id': 'entity_source_id',
            '_hash': '123'
        })
        event = dict_to_object(self.mock_event)
        with EventApplicator(self.storage, {}) as applicator:
            applicator.apply(event)
            self.assertEqual(len(applicator.add_events), 1)
            applicator.apply_all()
        self.assertEqual(len(applicator.add_events), 0)
        self.storage.get_entity_for_update.assert_not_called()
        self.storage.add_add_events.assert_called()

    def test_apply_new_add_exception(self):
        self.set_contents({
            '_entity_source_id': 'entity_source_id',
            '_hash': '123'
        })
        event = dict_to_object(self.mock_event)
        with self.assertRaises(GOBException):
            with EventApplicator(self.storage, {}) as applicator:
                applicator.apply(event)

    def test_apply_bulk(self):
        applicator = EventApplicator(self.storage, {})
        self.mock_event["action"] = 'BULKCONFIRM'
        self.set_contents({
            'confirms': [{
                '_entity_source_id': 'entity_source_id'
            }]
        })
        event = dict_to_object(self.mock_event)
        applicator.apply(event)
        self.assertEqual(len(applicator.add_events), 0)
        self.storage.bulk_update_confirms.assert_called()

    def test_apply_return_values(self):
        """This method tests that all applied events are correctly returned from apply

        :return:
        """
        applicator = EventApplicator(self.storage, {})
        applicator.add_add_event = MagicMock(return_value=['applied event1', 'applied event2'])
        applicator.apply_add_events = MagicMock(return_value=['applied event3', 'applied event4'])
        applicator.add_other_event = MagicMock(return_value=['applied event5', 'applied event6'])

        # BULKCONFIRM
        self.set_contents({
            'confirms': [{
                '_entity_source_id': 'entity_source_id'
            }]
        })
        self.mock_event['action'] = 'BULKCONFIRM'
        event = dict_to_object(self.mock_event)
        self.assertEqual((ANY, 1, []), applicator.apply(event))

        # ADD
        self.set_contents({
            '_entity_source_id': 'entity_source_id',
            '_hash': '123'
        })
        self.mock_event['action'] = 'ADD'
        event = dict_to_object(self.mock_event)
        self.assertEqual((ANY, 1, ['applied event1', 'applied event2']), applicator.apply(event))

        # CONFIRM (other)
        self.mock_event["action"] = 'CONFIRM'
        self.set_contents({
            '_entity_source_id': 'entity_source_id',
            '_hash': '123'
        })
        event = dict_to_object(self.mock_event)
        self.assertEqual((ANY, 1, ['applied event3', 'applied event4', 'applied event5', 'applied event6']), applicator.apply(event))

    def test_add_other_event(self):
        applicator = EventApplicator(self.storage, {})

        applicator.MAX_OTHER_CHUNK = 3
        applicator.apply_other_events = MagicMock()

        self.assertEqual([],
                         applicator.add_other_event('any gob event1', {'_entity_source_id': 'any entity source 1'}))
        self.assertEqual(applicator.other_events['any entity source 1'], 'any gob event1')

        self.assertEqual([],
                         applicator.add_other_event('any gob event2', {'_entity_source_id': 'any entity source 2'}))
        applicator.apply_other_events.assert_not_called()

        self.assertEqual(applicator.apply_other_events.return_value,
                         applicator.add_other_event('any gob event3', {'_entity_source_id': 'any entity source 3'}))
        applicator.apply_other_events.assert_called()

    def test_apply_other_events(self):
        applicator = EventApplicator(self.storage, {})
        applicator.apply_other_event = MagicMock()

        self.assertEqual(applicator.other_events, {})

        self.assertEqual([], applicator.apply_other_events())
        self.assertEqual(applicator.other_events, {})
        self.storage.get_entities.assert_not_called()

        applicator.add_other_event('any gob event', {'_entity_source_id': 'any entity source id'})
        self.storage.get_entities.return_value = ['any entity']

        self.assertEqual(['any gob event'], applicator.apply_other_events())
        self.assertEqual(applicator.other_events, {})
        self.storage.get_entities.assert_called()
        applicator.apply_other_event.assert_called_with('any entity')

    def test_apply_other_event(self):
        applicator = EventApplicator(self.storage, {})

        entity = MagicMock()
        entity._date_deleted = None
        entity._last_event = None

        gob_event = MagicMock()
        gob_event.id = 'any event id'

        entity._source_id = 'any source id'
        applicator.other_events['any source id'] = gob_event

        # Normal action, apply event and set last event id
        applicator.apply_other_event(entity)
        gob_event.apply_to.assert_called_with(entity)
        self.assertEqual(entity._last_event, gob_event.id)

        # Apply NON-ADD event on a deleted entity
        entity._date_deleted = 'any date deleted'
        with self.assertRaises(GOBException):
            applicator.apply_other_event(entity)

        # Apply ADD event on a deleted entity is OK
        gob_event = MagicMock(spec=GOB.ADD)
        gob_event.id = 'any event id'
        entity._last_event = None
        applicator.other_events['any source id'] = gob_event
        applicator.apply_other_event(entity)
        self.assertEqual(entity._last_event, gob_event.id)

        # Do not set last event id for CONFIRM events
        gob_event = MagicMock(spec=GOB.CONFIRM)
        gob_event.id = 'any event id'
        entity._last_event = None
        entity._date_deleted = None
        applicator.other_events['any source id'] = gob_event
        applicator.apply_other_event(entity)
        self.assertEqual(entity._last_event, None)

    def test_apply_all(self):
        applicator = EventApplicator(self.storage, {})
        applicator.apply_add_events = MagicMock(return_value=[1, 2])
        applicator.apply_other_events = MagicMock(return_value=[3])

        self.assertEqual([1, 2, 3], applicator.apply_all())
        applicator.apply_add_events.assert_called_once()
        applicator.apply_other_events.assert_called_once()
