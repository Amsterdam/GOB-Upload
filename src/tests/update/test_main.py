from unittest import TestCase

from gobupload.update.main import is_corrupted


class TestEventCollector(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_empty(self):
        entity_max_eventid = None
        last_eventid = None
        self.assertFalse(is_corrupted(entity_max_eventid, last_eventid))

    def test_failed_execution(self):
        entity_max_eventid = None
        last_eventid = 10
        self.assertFalse(is_corrupted(entity_max_eventid, last_eventid))

    def test_events_manipulated(self):
        entity_max_eventid = 10
        last_eventid = None
        self.assertTrue(is_corrupted(entity_max_eventid, last_eventid))

    def test_entities_manipulated(self):
        entity_max_eventid = 10
        last_eventid = 5
        self.assertTrue(is_corrupted(entity_max_eventid, last_eventid))
