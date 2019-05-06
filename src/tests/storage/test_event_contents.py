import datetime

import unittest
from unittest.mock import call, MagicMock, patch, Mock

from gobupload.storage.event_contents import loads, dumps


class TestEventContents(unittest.TestCase):

    def setUp(self):
        pass

    def test_plain(self):
        for data in [{}, {"some": "contents"}, {"a": 1}, {"b": True}]:
            contents = dumps(data)
            result_data = loads(contents)
            self.assertEqual(result_data, data)

    @patch('gobupload.storage.event_contents._compress')
    @patch('gobupload.storage.event_contents._decompress')
    def test_large_data(self, mock_decompress, mock_compress):
        mock_compress.return_value = "compress"
        mock_decompress.return_value = "{}"
        data = {
            "big": "a" * 1000,
            "bool": True,
            "number": 123
        }
        contents = dumps(data)
        mock_compress.assert_called()

        loads(contents)
        mock_decompress.assert_called_with("compress")

    def test_compress(self):
        data = {
            "big": "a" * 1000,
            "bool": True,
            "number": 123
        }
        contents = dumps(data)
        result_data = loads(contents)
        self.assertEqual(result_data, data)
