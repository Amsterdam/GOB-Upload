import datetime

from unittest import TestCase, mock
# from unittest.mock import MagicMock

# from gobupload.storage.handler import GOBStorageHandler
from gobupload.relate.relate import _handle_relations

# @mock('gobupload.relate.relate.GOBStorageHandler')
class TestRelate(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_empty(self):
        result = _handle_relations([])
        self.assertEqual(result, [])

class TestRelateNoStates(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_single(self):
        relations = [
            {'src__id': 'src_1', 'dst__id': 'dst_1'}
        ]
        expect = [
            {
                "src_id": {'id': 'src_1', 'volgnummer': None},
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{'id': 'dst_1', 'volgnummer': None}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_single_more(self):
        relations = [
            {'src__id': 'src_1', 'dst__id': 'dst_1'},
            {'src__id': 'src_2', 'dst__id': 'dst_2'}
        ]
        expect = [
            {
                "src_id": {'id': 'src_1', 'volgnummer': None},
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{'id': 'dst_1', 'volgnummer': None}]
            },
            {
                "src_id": {'id': 'src_2', 'volgnummer': None},
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{'id': 'dst_2', 'volgnummer': None}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_many(self):
        relations = [
            {'src__id': 'src_1', 'dst__id': 'dst_1'},
            {'src__id': 'src_1', 'dst__id': 'dst_2'}
        ]
        expect = [
            {
                "src_id": {'id': 'src_1', 'volgnummer': None},
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{'id': 'dst_1', 'volgnummer': None}, {'id': 'dst_2', 'volgnummer': None}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_many_more(self):
        relations = [
            {'src__id': 'src_1', 'dst__id': 'dst_1'},
            {'src__id': 'src_1', 'dst__id': 'dst_2'},
            {'src__id': 'src_2', 'dst__id': 'dst_3'},
            {'src__id': 'src_2', 'dst__id': 'dst_4'}
        ]
        expect = [
            {
                "src_id": {'id': 'src_1', 'volgnummer': None},
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{'id': 'dst_1', 'volgnummer': None}, {'id': 'dst_2', 'volgnummer': None}]
            },
            {
                "src_id": {'id': 'src_2', 'volgnummer': None},
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{'id': 'dst_3', 'volgnummer': None}, {'id': 'dst_4', 'volgnummer': None}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

class TestRelateBothStates(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_single(self):
        relations = [
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'id': 'dst_1', 'volgnummer': '1'}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_empty_start(self):
        relations = [
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2007, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2007, 1, 1),
                'dst': []
            },
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2007, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'id': 'dst_1', 'volgnummer': '1'}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_empty_end(self):
        relations = [
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2010, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2010, 1, 1),
                'dst': [{'id': 'dst_1', 'volgnummer': '1'}]
            },
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2010, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': []
            }
        ]
        result = _handle_relations(relations)
        print("RESULT", result)
        self.assertEqual(result, expect)

    def test_empty_begin_end(self):
        relations = [
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2007, 1, 1),
                'dst_eind_geldigheid': datetime.date(2010, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2007, 1, 1),
                'dst': []
            },
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2007, 1, 1),
                'eind_geldigheid': datetime.date(2010, 1, 1),
                'dst': [{'id': 'dst_1', 'volgnummer': '1'}]
            },
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2010, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': []
            }
        ]
        result = _handle_relations(relations)
        print("RESULT", result)
        self.assertEqual(result, expect)

    def test_before(self):
        relations = [
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2000, 1, 1),
                'dst_eind_geldigheid': datetime.date(2006, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': []
            }
        ]
        result = _handle_relations(relations)
        print("RESULT", result)
        self.assertEqual(result, expect)

    def test_after(self):
        relations = [
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2011, 1, 1),
                'dst_eind_geldigheid': datetime.date(2016, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': []
            }
        ]
        result = _handle_relations(relations)
        print("RESULT", result)
        self.assertEqual(result, expect)

    def test_before_and_after(self):
        relations = [
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2001, 1, 1),
                'dst_eind_geldigheid': datetime.date(2016, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'id': 'dst_1', 'volgnummer': '1'}]
            }
        ]
        result = _handle_relations(relations)
        print("RESULT", result)
        self.assertEqual(result, expect)

    def test_more(self):
        relations = [
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2007, 1, 1)
            },
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_1',
                'dst_volgnummer': '2',
                'dst_begin_geldigheid': datetime.date(2007, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2007, 1, 1),
                'dst': [{'id': 'dst_1', 'volgnummer': '1'}]
            },
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2007, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'id': 'dst_1', 'volgnummer': '2'}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_many(self):
        relations = [
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            },
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_2',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'id': 'dst_1', 'volgnummer': '1'}, {'id': 'dst_2', 'volgnummer': '1'}]
            }
        ]
        result = _handle_relations(relations)
        print("RESULT", result)
        self.assertEqual(result, expect)

    def test_many_with_diff(self):
        relations = [
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2008, 1, 1)
            },
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_2',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2008, 1, 1),
                'dst': [{'id': 'dst_1', 'volgnummer': '1'}, {'id': 'dst_2', 'volgnummer': '1'}]
            },
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2008, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'id': 'dst_2', 'volgnummer': '1'}]
            }
        ]
        result = _handle_relations(relations)
        print("RESULT", result)
        self.assertEqual(result, expect)

class TestRelateNoStatesWithStates(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_single(self):
        relations = [
            {
                'src__id': 'src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'id': 'src_1', 'volgnummer': None},
                'begin_geldigheid': None,
                'eind_geldigheid': datetime.date(2006, 1, 1),
                'dst': []
            },
            {
                'src_id': {'id': 'src_1', 'volgnummer': None},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'id': 'dst_1', 'volgnummer': '1'}]
            },
            {
                'src_id': {'id': 'src_1', 'volgnummer': None},
                'begin_geldigheid': datetime.date(2011, 1, 1),
                'eind_geldigheid': None,
                'dst': []
            }
        ]
        result = _handle_relations(relations)
        print("RESULT", result)
        self.assertEqual(result, expect)

class TestRelateWithStatesNoStates(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_single(self):
        relations = [
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2005, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_1'
            }
        ]
        expect = [
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2005, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'id': 'dst_1', 'volgnummer': None}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_many(self):
        relations = [
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2005, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_1'
            },
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2005, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_2'
            }
        ]
        expect = [
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2005, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'id': 'dst_1', 'volgnummer': None}, {'id': 'dst_2', 'volgnummer': None}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_multiple_volgnummer(self):
        relations = [
            {
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2005, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_1'
            },
            {
                'src__id': 'src_1',
                'src_volgnummer': '2',
                'src_begin_geldigheid': datetime.date(2007, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__id': 'dst_2'
            }
        ]
        expect = [
            {
                'src_id': {'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2005, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'id': 'dst_1', 'volgnummer': None}]
            },
            {
                'src_id': {'id': 'src_1', 'volgnummer': '2'},
                'begin_geldigheid': datetime.date(2007, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'id': 'dst_2', 'volgnummer': None}]
            }
        ]
        result = _handle_relations(relations)
        print("RESULT", result)
        self.assertEqual(result, expect)
