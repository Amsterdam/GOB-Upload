from unittest import TestCase
from unittest.mock import MagicMock

from gobcore.exceptions import GOBException
from gobupload.storage.handler import GOBStorageHandler
from gobupload.update.event_collector import EventCollector


class TestEventCollector(TestCase):

    def setUp(self):
        self.storage = MagicMock(spec=GOBStorageHandler)

    def test_constructor(self):
        collector = EventCollector(self.storage, {})
        assert collector.events == []
        assert collector.last_events == {}
        assert collector.storage == self.storage

    def test_collect(self):
        collector = EventCollector(self.storage, {})
        collector.collect({'event': 'any event'})
        assert collector.events == [{'event': 'any event'}]

    def test_is_valid(self):
        last_events = {"any tid": 100}
        collector = EventCollector(self.storage, last_events)

        event = {
            "event": "event",
            "data": {
                "_tid": "any tid",
                "_last_event": 100
            }
        }
        assert collector.is_valid(event)

        event["data"]["_last_event"] = 50
        assert collector.is_valid(event) is False

        event["data"]["_tid"] = 2
        assert collector.is_valid(event) is False

    def test_exit(self):
        event = {"event": "event", "data": {"_tid": 1, "_last_event": 100}}

        # store_events not called, raise GOBException
        with self.assertRaises(GOBException):
            with EventCollector(self.storage, {}) as collector:
                collector.collect(event)

        # store_events called, assert events cleared
        with EventCollector(self.storage, {}) as collector:
            collector.collect(event)
            collector.store_events()

        assert len(collector.events) == 0

