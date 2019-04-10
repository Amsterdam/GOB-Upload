from unittest import TestCase

from gobupload.compare.event_collector import EventCollector

class TestEventCollector(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_add_empty(self):
        result = None
        with EventCollector() as ec:
            result = ec.events
        self.assertEqual(result, [])

    def test_add_one(self):
        result = None
        with EventCollector() as ec:
            ec.add({"event": 1})
            result = ec.events
        self.assertEqual(result, [{"event": 1}])

    def test_add_multiple(self):
        result = None
        with EventCollector() as ec:
            ec.add({"event": 1})
            ec.add({"event": 2})
            ec.add({"event": 3})
            result = ec.events
        self.assertEqual(result, [{"event": 1}, {"event": 2}, {"event": 3}])

    def test_add_bulk_one(self):
        confirm_event = {
            "event": "CONFIRM",
            "data": {
                "_source_id": "source_id",
                "_last_event": "last_event"
            }
        }

        result = None
        with EventCollector() as ec:
            ec.add(confirm_event)
            result = ec.events
        self.assertEqual(result, [confirm_event])

    def test_add_bulk_multi(self):
        confirm_event = {
            "event": "CONFIRM",
            "data": {
                "_source_id": "source_id",
                "_last_event": "last_event"
            }
        }
        expectation = {
            "event": "BULKCONFIRM",
            "data": {
                "_entity_source_id": None,
                "_source_id": None,
                "confirms": [
                    confirm_event["data"],
                    confirm_event["data"]
                ]
            }
        }

        result = None
        with EventCollector() as ec:
            ec.add(confirm_event)
            ec.add(confirm_event)
            result = ec.events
        self.assertEqual(result, [expectation])

        EventCollector.MAX_BULK = 2
        result = None
        with EventCollector() as ec:
            ec.add(confirm_event)
            ec.add(confirm_event)
            ec.add(confirm_event)
            result = ec.events
        self.assertEqual(result, [expectation, confirm_event])
