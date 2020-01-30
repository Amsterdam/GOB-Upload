import datetime

from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobupload.relate.relate import relate_update
from gobupload.storage.relate import _convert_row



@patch('gobupload.relate.relate.logger', MagicMock())
class TestRelateDateTime(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_without_time_with_time(self):
        row = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.date(2005, 1, 1),
            'src_eind_geldigheid': datetime.date(2011, 1, 1),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': datetime.datetime(2006, 1, 1, 12, 0, 0),
            'dst_eind_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0)
        }
        expect = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.datetime(2005, 1, 1, 0, 0, 0),
            'src_eind_geldigheid': datetime.datetime(2011, 1, 1, 0, 0, 0),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': datetime.datetime(2006, 1, 1, 12, 0, 0),
            'dst_eind_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0)
        }
        result = _convert_row(row)
        self.assertEqual(result, expect)

    def test_with_time_without_time(self):
        row = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.datetime(2005, 1, 1, 12, 0, 0),
            'src_eind_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': datetime.date(2006, 1, 1),
            'dst_eind_geldigheid': datetime.date(2011, 1, 1)
        }
        expect = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.datetime(2005, 1, 1, 12, 0, 0),
            'src_eind_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': datetime.datetime(2006, 1, 1, 0, 0, 0),
            'dst_eind_geldigheid': datetime.datetime(2011, 1, 1, 0, 0, 0)
        }
        result = _convert_row(row)
        self.assertEqual(result, expect)

    def test_with_datetime_no_date(self):
        row = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.datetime(2005, 1, 1, 12, 0, 0),
            'src_eind_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': None,
            'dst_eind_geldigheid': None
        }
        expect = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.datetime(2005, 1, 1, 12, 0, 0),
            'src_eind_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': None,
            'dst_eind_geldigheid': None
        }
        result = _convert_row(row)
        self.assertEqual(result, expect)

    def test_with_date_no_date(self):
        row = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.date(2005, 1, 1),
            'src_eind_geldigheid': datetime.date(2011, 1, 1),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': None,
            'dst_eind_geldigheid': None
        }
        expect = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.date(2005, 1, 1),
            'src_eind_geldigheid': datetime.date(2011, 1, 1),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': None,
            'dst_eind_geldigheid': None
        }
        result = _convert_row(row)
        self.assertEqual(result, expect)

    def test_without_time_without_time(self):
        row = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.date(2005, 1, 1),
            'src_eind_geldigheid': datetime.date(2011, 1, 1),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': datetime.date(2006, 1, 1),
            'dst_eind_geldigheid': datetime.date(2011, 1, 1)
        }
        expect = {
            'src__source': 'src_src_1',
            'src__id': 'src_1',
            'src_volgnummer': '1',
            'src_begin_geldigheid': datetime.date(2005, 1, 1),
            'src_eind_geldigheid': datetime.date(2011, 1, 1),
            'dst__source': 'dst_src_1',
            'dst__id': 'dst_1',
            'dst_volgnummer': '1',
            'dst_begin_geldigheid': datetime.date(2006, 1, 1),
            'dst_eind_geldigheid': datetime.date(2011, 1, 1)
        }
        result = _convert_row(row)
        self.assertEqual(result, expect)

    @patch("gobupload.relate.relate.update_relations")
    def test_relate_update(self, mock_update_relations):
        mock_update_relations.return_value = "filename", 24
        self.assertEqual(("filename", 24), relate_update("any catalog", "any collection", "any reference"))
        mock_update_relations.assert_called()
