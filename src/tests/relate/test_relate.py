import datetime

from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from gobcore.model import GOBModel
from gobcore.sources import GOBSources

from gobupload.storage.handler import GOBStorageHandler

from gobupload.relate.relate import relate, _handle_relations
from gobupload.storage.relate import get_relations, _get_data, _convert_row

class TestRelations(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('gobupload.storage.relate._get_data')
    def test_no_states_no_states(self, mock_get_data):
        mock._get_data = MagicMock()
        mock_get_collection = lambda *args: {
            'all_fields': {
                'field': {
                    'type': 'GOB.Reference',
                    'ref': 'dst_catalogue:dst_collection'
                }
            }
        }
        mock_get_field_relations = lambda *args: [
            {
                'source': 'src_application',
                'field_name': 'src_attr',
                'destination_attribute': 'dst_attr'
            }
        ]
        expect = """
SELECT
    src._source AS src__source,
    src._id AS src__id,
    dst._source AS dst__source,
    dst._id AS dst__id
FROM catalog_collection AS src
LEFT OUTER JOIN (
SELECT
    _source,
    _id,
    dst_attr
FROM dst_catalogue_dst_collection) AS dst
ON
    (src._application = 'src_application' AND dst.dst_attr = src.src_attr->>'bronwaarde')
ORDER BY
    src._id
"""
        with patch.object(GOBSources, 'get_field_relations', mock_get_field_relations), \
             patch.object(GOBModel, 'get_collection', mock_get_collection):
             get_relations("catalog", "collection", "field")
        mock_get_data.assert_called_with(expect)

    @patch('gobupload.storage.relate._get_data')
    def test_with_states_with_states(self, mock_get_data):
        mock._get_data = MagicMock()
        mock_get_collection = lambda *args: {
            'all_fields': {
                'field': {
                    'type': 'GOB.Reference',
                    'ref': 'dst_catalogue:dst_collection'
                }
            }
        }
        mock_get_field_relations = lambda *args: [
            {
                'source': 'src_application',
                'field_name': 'src_attr',
                'destination_attribute': 'dst_attr'
            }
        ]
        expect = """
SELECT
    src._source AS src__source,
    src._id AS src__id,
    src.volgnummer AS src_volgnummer,
    src.begin_geldigheid AS src_begin_geldigheid,
    src.eind_geldigheid AS src_eind_geldigheid,
    dst._source AS dst__source,
    dst._id AS dst__id,
    dst.volgnummer AS dst_volgnummer,
    dst.begin_geldigheid AS dst_begin_geldigheid,
    dst.eind_geldigheid AS dst_eind_geldigheid
FROM catalog_collection AS src
LEFT OUTER JOIN (
SELECT
    _source,
    _id,
    volgnummer,
    begin_geldigheid,
    eind_geldigheid,
    dst_attr
FROM dst_catalogue_dst_collection) AS dst
ON
    (src._application = 'src_application' AND dst.dst_attr = src.src_attr->>'bronwaarde') AND
    (dst.begin_geldigheid < src.eind_geldigheid OR src.eind_geldigheid is NULL) AND
    (dst.eind_geldigheid > src.begin_geldigheid OR dst.eind_geldigheid is NULL)
ORDER BY
    src._id, src.volgnummer, src.begin_geldigheid, dst.begin_geldigheid, dst.eind_geldigheid
"""
        with patch.object(GOBSources, 'get_field_relations', mock_get_field_relations), \
             patch.object(GOBModel, 'get_collection', mock_get_collection),\
             patch.object(GOBModel, 'has_states', lambda *args: True):
            get_relations("catalog", "collection", "field")
        mock_get_data.assert_called_with(expect)

    @patch('gobupload.storage.relate._get_data')
    def test_many_reference(self, mock_get_data):
        mock._get_data = MagicMock()
        mock_get_collection = lambda *args: {
            'all_fields': {
                'field': {
                    'type': 'GOB.ManyReference',
                    'ref': 'dst_catalogue:dst_collection'
                }
            }
        }
        mock_get_field_relations = lambda *args: [
            {
                'source': 'src_application',
                'field_name': 'src_attr',
                'destination_attribute': 'dst_attr'
            }
        ]
        expect = """
SELECT
    src._source AS src__source,
    src._id AS src__id,
    src.volgnummer AS src_volgnummer,
    src.begin_geldigheid AS src_begin_geldigheid,
    src.eind_geldigheid AS src_eind_geldigheid,
    dst._source AS dst__source,
    dst._id AS dst__id,
    dst.volgnummer AS dst_volgnummer,
    dst.begin_geldigheid AS dst_begin_geldigheid,
    dst.eind_geldigheid AS dst_eind_geldigheid
FROM catalog_collection AS src
LEFT OUTER JOIN (
SELECT
    _source,
    _id,
    volgnummer,
    begin_geldigheid,
    eind_geldigheid,
    dst_attr
FROM dst_catalogue_dst_collection) AS dst
ON
    (src._application = 'src_application' AND dst.dst_attr = ANY(ARRAY(SELECT x->>'bronwaarde' FROM jsonb_array_elements(src.src_attr) as x))) AND
    (dst.begin_geldigheid < src.eind_geldigheid OR src.eind_geldigheid is NULL) AND
    (dst.eind_geldigheid > src.begin_geldigheid OR dst.eind_geldigheid is NULL)
ORDER BY
    src._id, src.volgnummer, src.begin_geldigheid, dst.begin_geldigheid, dst.eind_geldigheid
"""
        with patch.object(GOBSources, 'get_field_relations', mock_get_field_relations), \
             patch.object(GOBModel, 'get_collection', mock_get_collection), \
             patch.object(GOBModel, 'has_states', lambda *args: True):
            get_relations("catalog", "collection", "field")
        mock_get_data.assert_called_with(expect)

    @patch('gobupload.storage.relate._get_data')
    def test_more_matches(self, mock_get_data):
        mock._get_data = MagicMock()
        mock_get_collection = lambda *args: {
            'all_fields': {
                'field': {
                    'type': 'GOB.Reference',
                    'ref': 'dst_catalogue:dst_collection'
                }
            }
        }
        mock_get_field_relations = lambda *args: [
            {
                'source': 'src_application1',
                'field_name': 'src_attr1',
                'destination_attribute': 'dst_attr1'
            },
            {
                'source': 'src_application2',
                'field_name': 'src_attr2',
                'destination_attribute': 'dst_attr2'
            }
        ]
        expect = """
SELECT
    src._source AS src__source,
    src._id AS src__id,
    src.volgnummer AS src_volgnummer,
    src.begin_geldigheid AS src_begin_geldigheid,
    src.eind_geldigheid AS src_eind_geldigheid,
    dst._source AS dst__source,
    dst._id AS dst__id,
    dst.volgnummer AS dst_volgnummer,
    dst.begin_geldigheid AS dst_begin_geldigheid,
    dst.eind_geldigheid AS dst_eind_geldigheid
FROM catalog_collection AS src
LEFT OUTER JOIN (
SELECT
    _source,
    _id,
    volgnummer,
    begin_geldigheid,
    eind_geldigheid,
    dst_attr1,
    dst_attr2
FROM dst_catalogue_dst_collection) AS dst
ON
    ((src._application = 'src_application1' AND dst.dst_attr1 = src.src_attr1->>'bronwaarde') OR
    (src._application = 'src_application2' AND dst.dst_attr2 = src.src_attr2->>'bronwaarde')) AND
    (dst.begin_geldigheid < src.eind_geldigheid OR src.eind_geldigheid is NULL) AND
    (dst.eind_geldigheid > src.begin_geldigheid OR dst.eind_geldigheid is NULL)
ORDER BY
    src._id, src.volgnummer, src.begin_geldigheid, dst.begin_geldigheid, dst.eind_geldigheid
"""
        with patch.object(GOBSources, 'get_field_relations', mock_get_field_relations), \
             patch.object(GOBModel, 'get_collection', mock_get_collection), \
             patch.object(GOBModel, 'has_states', lambda *args: True):
            get_relations("catalog", "collection", "field")
        mock_get_data.assert_called_with(expect)


    @patch('gobupload.storage.relate.GOBStorageHandler', MagicMock())
    def test_get_data(self):
        result = _get_data('')
        self.assertEqual(result, [])


class TestRelate(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # def test_empty(self):
    #     result = _handle_relations([])
    #     self.assertEqual(result, [])

    @patch('gobupload.relate.relate.get_relations')
    def test_relate(self, mock_get_relations):
        mock_get_relations.return_value = []
        result = relate("catalog", "collection", "field")
        self.assertEqual(result, [])
        mock_get_relations.assert_called_with("catalog", "collection", "field")


class TestRelateNoStates(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_single(self):
        relations = [
            {'src__source': 'src_src_1', 'src__id': 'src_1', 'dst__source': 'src_dst_1', 'dst__id': 'dst_1'}
        ]
        expect = [
            {
                "src_id": {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': None},
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{'source': 'src_dst_1', 'id': 'dst_1', 'volgnummer': None}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_single_more(self):
        relations = [
            {'src__source': 'src_src_1', 'src__id': 'src_1', 'dst__source': 'src_dst_1', 'dst__id': 'dst_1'},
            {'src__source': 'src_src_1', 'src__id': 'src_2', 'dst__source': 'src_dst_1', 'dst__id': 'dst_2'}
        ]
        expect = [
            {
                "src_id": {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': None},
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{'source': 'src_dst_1', 'id': 'dst_1', 'volgnummer': None}]
            },
            {
                "src_id": {'source': 'src_src_1', 'id': 'src_2', 'volgnummer': None},
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{'source': 'src_dst_1', 'id': 'dst_2', 'volgnummer': None}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_many(self):
        relations = [
            {'src__source': 'src_src_1', 'src__id': 'src_1', 'dst__source': 'src_dst_1', 'dst__id': 'dst_1'},
            {'src__source': 'src_src_1', 'src__id': 'src_1', 'dst__source': 'src_dst_1', 'dst__id': 'dst_2'}
        ]
        expect = [
            {
                "src_id": {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': None},
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{'source': 'src_dst_1', 'id': 'dst_1', 'volgnummer': None}, {'source': 'src_dst_1', 'id': 'dst_2', 'volgnummer': None}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_many_more(self):
        relations = [
            {'src__source': 'src_src_1', 'src__id': 'src_1', 'dst__source': 'src_dst_1', 'dst__id': 'dst_1'},
            {'src__source': 'src_src_1', 'src__id': 'src_1', 'dst__source': 'src_dst_1', 'dst__id': 'dst_2'},
            {'src__source': 'src_src_1', 'src__id': 'src_2', 'dst__source': 'src_dst_1', 'dst__id': 'dst_3'},
            {'src__source': 'src_src_1', 'src__id': 'src_2', 'dst__source': 'src_dst_1', 'dst__id': 'dst_4'}
        ]
        expect = [
            {
                "src_id": {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': None},
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{'source': 'src_dst_1', 'id': 'dst_1', 'volgnummer': None}, {'source': 'src_dst_1', 'id': 'dst_2', 'volgnummer': None}]
            },
            {
                "src_id": {'source': 'src_src_1', 'id': 'src_2', 'volgnummer': None},
                "begin_geldigheid": None,
                "eind_geldigheid": None,
                "dst": [{'source': 'src_dst_1', 'id': 'dst_3', 'volgnummer': None}, {'source': 'src_dst_1', 'id': 'dst_4', 'volgnummer': None}]
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
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_1', 'volgnummer': '1'}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_empty_start(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2007, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2007, 1, 1),
                'dst': []
            },
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2007, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_1', 'volgnummer': '1'}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_empty_end(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2010, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2010, 1, 1),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_1', 'volgnummer': '1'}]
            },
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2010, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': []
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_empty_begin_end(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2007, 1, 1),
                'dst_eind_geldigheid': datetime.date(2010, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2007, 1, 1),
                'dst': []
            },
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2007, 1, 1),
                'eind_geldigheid': datetime.date(2010, 1, 1),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_1', 'volgnummer': '1'}]
            },
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2010, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': []
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_before(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2000, 1, 1),
                'dst_eind_geldigheid': datetime.date(2006, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': []
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_after(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2011, 1, 1),
                'dst_eind_geldigheid': datetime.date(2016, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': []
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_before_and_after(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2001, 1, 1),
                'dst_eind_geldigheid': datetime.date(2016, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_1', 'volgnummer': '1'}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_more(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2007, 1, 1)
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '2',
                'dst_begin_geldigheid': datetime.date(2007, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2007, 1, 1),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_1', 'volgnummer': '1'}]
            },
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2007, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_1', 'volgnummer': '2'}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_many(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2012, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2012, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_2',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_2',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2012, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_2',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2012, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_1', 'volgnummer': '1'}, {'source': 'dst_src_1', 'id': 'dst_2', 'volgnummer': '1'}]
            },
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2011, 1, 1),
                'eind_geldigheid': datetime.date(2012, 1, 1),
                'dst': []
            },
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_2', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2012, 1, 1),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_2', 'volgnummer': '1'}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_many_with_diff(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2008, 1, 1)
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2006, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_2',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2008, 1, 1),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_1', 'volgnummer': '1'}, {'source': 'dst_src_1', 'id': 'dst_2', 'volgnummer': '1'}]
            },
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2008, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_2', 'volgnummer': '1'}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

class TestRelateNoStatesWithStates(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_single(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.date(2006, 1, 1),
                'dst_eind_geldigheid': datetime.date(2011, 1, 1)
            }
        ]
        expect = [
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': None},
                'begin_geldigheid': None,
                'eind_geldigheid': datetime.date(2006, 1, 1),
                'dst': []
            },
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': None},
                'begin_geldigheid': datetime.date(2006, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_1', 'volgnummer': '1'}]
            },
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': None},
                'begin_geldigheid': datetime.date(2011, 1, 1),
                'eind_geldigheid': None,
                'dst': []
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

class TestRelateWithStatesNoStates(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_single(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2005, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1'
            }
        ]
        expect = [
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2005, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_1', 'volgnummer': None}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_many(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2005, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1'
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2005, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_2'
            }
        ]
        expect = [
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2005, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_1', 'volgnummer': None},
                        {'source': 'dst_src_1', 'id': 'dst_2', 'volgnummer': None}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

    def test_multiple_volgnummer(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '1',
                'src_begin_geldigheid': datetime.date(2005, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1'
            },
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'src_volgnummer': '2',
                'src_begin_geldigheid': datetime.date(2007, 1, 1),
                'src_eind_geldigheid': datetime.date(2011, 1, 1),
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_2'
            }
        ]
        expect = [
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '1'},
                'begin_geldigheid': datetime.date(2005, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_1', 'volgnummer': None}]
            },
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': '2'},
                'begin_geldigheid': datetime.date(2007, 1, 1),
                'eind_geldigheid': datetime.date(2011, 1, 1),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_2', 'volgnummer': None}]
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

class TestRelateDateTime(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_single(self):
        relations = [
            {
                'src__source': 'src_src_1',
                'src__id': 'src_1',
                'dst__source': 'dst_src_1',
                'dst__id': 'dst_1',
                'dst_volgnummer': '1',
                'dst_begin_geldigheid': datetime.datetime(2006, 1, 1, 12, 0, 0),
                'dst_eind_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0)
            }
        ]
        expect = [
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': None},
                'begin_geldigheid': None,
                'eind_geldigheid': datetime.datetime(2006, 1, 1, 12, 0, 0),
                'dst': []
            },
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': None},
                'begin_geldigheid': datetime.datetime(2006, 1, 1, 12, 0, 0),
                'eind_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0),
                'dst': [{'source': 'dst_src_1', 'id': 'dst_1', 'volgnummer': '1'}]
            },
            {
                'src_id': {'source': 'src_src_1', 'id': 'src_1', 'volgnummer': None},
                'begin_geldigheid': datetime.datetime(2011, 1, 1, 12, 0, 0),
                'eind_geldigheid': None,
                'dst': []
            }
        ]
        result = _handle_relations(relations)
        self.assertEqual(result, expect)

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
