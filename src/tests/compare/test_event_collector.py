from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.message_broker.offline_contents import ContentsWriter
from gobupload.compare.event_collector import EventCollector

mock_writer = MagicMock(spec=ContentsWriter)
mock_confirms_writer = MagicMock(spec=ContentsWriter)

@patch('gobupload.compare.main.ContentsWriter', mock_writer)
class TestEventCollector(TestCase):

    def setUp(self):
        mock_writer.reset_mock()
        mock_confirms_writer.reset_mock()

    def tearDown(self):
        pass

    def test_add_empty(self):
        with EventCollector(mock_writer, mock_confirms_writer) as ec:
            pass
        mock_writer.assert_not_called()

    def test_add_one(self):
        with EventCollector(mock_writer, mock_confirms_writer) as ec:
            ec.collect({"event": 1})
        mock_writer.write.assert_called_once()
        mock_writer.write.assert_called_with({"event": 1})

    def test_add_multiple(self):
        with EventCollector(mock_writer, mock_confirms_writer) as ec:
            ec.collect({"event": 1})
            mock_writer.write.assert_called_with({"event": 1})
            ec.collect({"event": 2})
            mock_writer.write.assert_called_with({"event": 2})
            ec.collect({"event": 3})
            mock_writer.write.assert_called_with({"event": 3})

    def test_add_bulk_one(self):
        confirm_event = {
            "event": "CONFIRM",
            "data": {
                "_source_id": "source_id",
                "_last_event": "last_event"
            }
        }

        with EventCollector(mock_writer, mock_confirms_writer) as ec:
            ec.collect(confirm_event)
        mock_writer.write.assert_not_called()
        mock_confirms_writer.write.assert_called_once()
        mock_confirms_writer.write.assert_called_with(confirm_event)

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

        with EventCollector(mock_writer, mock_confirms_writer) as ec:
            ec.collect(confirm_event)
            ec.collect(confirm_event)
        mock_writer.write.assert_not_called()
        mock_confirms_writer.write.assert_called_once()
        mock_confirms_writer.write.assert_called_with(expectation)

        mock_writer.reset_mock()

        EventCollector.MAX_BULK = 2
        with EventCollector(mock_writer, mock_confirms_writer) as ec:
            ec.collect(confirm_event)
            ec.collect(confirm_event)
            mock_confirms_writer.write.assert_called_with(expectation)
            ec.collect(confirm_event)
        mock_confirms_writer.write.assert_called_with(confirm_event)
