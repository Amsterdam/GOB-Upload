import json
from unittest import TestCase
from unittest.mock import MagicMock, patch, call, ANY

from gobupload.events_file.__main__ import EventsFileWriter, main


class TestEventsFileWriter(TestCase):

    @patch("gobupload.events_file.__main__._get_gob_event")
    def test_db_to_gob_event(self, mock_get_gob_event):
        mock_event = MagicMock()
        mock_event.contents = {'some': 'dict'}

        writer = EventsFileWriter('')

        self.assertEqual(mock_get_gob_event.return_value, writer._db_to_gob_event(mock_event))
        mock_get_gob_event.assert_called_with(mock_event, {'some': 'dict'})

        mock_event.contents = json.dumps({'some': 'other dict'})
        self.assertEqual(mock_get_gob_event.return_value, writer._db_to_gob_event(mock_event))
        mock_get_gob_event.assert_called_with(mock_event, {'some': 'other dict'})

    @patch("builtins.open")
    @patch("gobupload.events_file.__main__.GOBStorageHandler")
    def test_write_events(self, mock_storage_handler, mock_open):
        events = [
            {'type': 'ADD', 'id': 1, '_source_id': '100.1'},
            {'type': 'MODIFY', 'id': 2, '_source_id': '200.1'},
        ]

        mock_filter_by = mock_storage_handler.return_value. \
            get_session.return_value.__enter__.return_value. \
            query.return_value. \
            yield_per.return_value. \
            filter_by

        mock_order_by = mock_filter_by.return_value.order_by
        mock_order_by.return_value = events

        mock_file = mock_open.return_value.__enter__.return_value

        writer = EventsFileWriter('')
        writer._db_to_gob_event = lambda ev: type('Event', (), {
            'name': ev['type'],
            'id': ev['id'],
            '_data': ev,
            'catalogue': 'the cat',
            'entity': 'the col',
            'source': 'the source',
            'last_event': 2480,
        })

        writer._write_events('CAT', 'COL', '/dst/dir')
        mock_open.assert_called_with('/dst/dir/CAT_COL.gobevents', 'w')

        written_event_headers = [{
            'event_type': 'ADD',
            'event_id': 1,
            'source_id': '100.1',
            'last_event': 2480,
            'catalog': 'the cat',
            'collection': 'the col',
            'source': 'the source',
        }, {
            'event_type': 'MODIFY',
            'event_id': 2,
            'source_id': '200.1',
            'last_event': 2480,
            'catalog': 'the cat',
            'collection': 'the col',
            'source': 'the source',
        }]
        mock_file.write.assert_has_calls([
            call(f'100.1|{json.dumps(written_event_headers[0])}|{json.dumps(events[0])}\n'),
            call(f'200.1|{json.dumps(written_event_headers[1])}|{json.dumps(events[1])}\n'),
        ])
        mock_filter_by.assert_called_with(catalogue='CAT', entity='COL')
        mock_order_by.assert_called_with(mock_storage_handler().DbEvent.eventid.asc())

    @patch("gobupload.events_file.__main__.os.walk")
    @patch("gobupload.events_file.__main__.zipfile")
    def test_zipdir(self, mock_zipfile, mock_os_walk):
        mock_os_walk.return_value = [
            ('root1', [], ['file1', 'file2']),
            ('root2', [], ['file3']),
        ]

        writer = EventsFileWriter('')
        writer._zipdir('the dir', 'thezipfile.zip')

        mock_zipfile.ZipFile.assert_called_with('thezipfile.zip', 'w', mock_zipfile.ZIP_DEFLATED)
        mock_os_walk.assert_called_with('the dir')

        mock_zipfile.ZipFile.return_value.write.assert_has_calls([
            call('root1/file1'),
            call('root1/file2'),
            call('root2/file3'),
        ])
        mock_zipfile.ZipFile.return_value.close.assert_called_once()

    @patch("gobupload.events_file.__main__.GOBModel")
    @patch("gobupload.events_file.__main__.shutil.rmtree")
    @patch("gobupload.events_file.__main__.os.makedirs")
    def test_write(self, mock_makedirs, mock_rmtree, mock_model):
        mock_model().get_collection_names = lambda cat: {
            'CAT': ['coll1', 'coll2']
        }[cat]
        writer = EventsFileWriter('CAT')
        writer._write_events = MagicMock()
        writer._zipdir = MagicMock()

        # To assert correct order
        dir_mocks = MagicMock()
        dir_mocks.attach_mock(mock_rmtree, 'rmtree')
        dir_mocks.attach_mock(mock_makedirs, 'makedirs')

        writer.write()

        dir_mocks.assert_has_calls([
            call.rmtree('gobevents/CAT'),
            call.makedirs('gobevents/CAT', exist_ok=True)
        ])

        writer._write_events.assert_has_calls([
            call('CAT', 'coll1', 'gobevents/CAT'),
            call('CAT', 'coll2', 'gobevents/CAT'),
        ])

        writer._zipdir.assert_not_called()

        # Try with collection, rmtree FileNotFoundError and zip
        writer._write_events.reset_mock()
        mock_rmtree.side_effect = FileNotFoundError
        writer.collection = 'COLL'
        writer.zip = True

        writer.write()

        writer._write_events.assert_called_once_with('CAT', 'COLL', 'gobevents/CAT')
        writer._zipdir.assert_called_with('gobevents/CAT', 'gobevents/CAT.zip')

    @patch("gobupload.events_file.__main__.argparse.ArgumentParser")
    @patch("gobupload.events_file.__main__.EventsFileWriter")
    def test_main(self, mock_writer, mock_parser):
        main()

        mock_parser().add_argument.assert_has_calls([
            call('catalog', type=str, help=ANY),
            call('collection', type=str, nargs='?', help=ANY),
            call('--zip', dest='zip', action='store_true', help=ANY),
        ])

        mock_args = mock_parser().parse_args()
        mock_writer.assert_called_with(mock_args.catalog, mock_args.collection, mock_args.zip)
        mock_writer().write.assert_called_once()
