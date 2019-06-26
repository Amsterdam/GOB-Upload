from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from gobcore.model import GOBModel
from gobcore.sources import GOBSources

from gobupload.storage.relate import get_relations, update_relations, EQUALS, LIES_IN, JOIN, WHERE, _update_match, _get_data, get_last_change, get_current_relations, RelationUpdater, _query_missing, check_relations

@patch('gobupload.relate.relate.logger', MagicMock())
class TestRelations(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('gobupload.storage.relate._execute')
    def test_last_change(self, mock_execute):
        result = get_last_change("catalog", "collection")
        # self.assertEqual(result, "")
        mock_execute.assert_called_with("""
SELECT eventid
FROM   events
WHERE  catalogue = 'catalog' AND
       entity = 'collection' AND
       action != 'CONFIRM'
ORDER BY timestamp DESC
LIMIT 1
""")

    @patch('gobupload.storage.relate._execute_multiple')
    def test_relation_updater(self, _execute_multiple):
        updater = RelationUpdater("catalog", "collection")
        updater.update("field", {"field": "field", "_gobid": "_gobid"})
        self.assertEqual(updater.queries, ["""
UPDATE catalog_collection
SET    field = $quotedString$"field"$quotedString$
WHERE  _gobid = _gobid
"""
        ])
        RelationUpdater.UPDATE_INTERVAL = 1
        updater.update("field", {"field": "field", "_gobid": "_gobid"})
        _execute_multiple.assert_called()


    @patch('gobupload.storage.relate._execute')
    def test_current_relations(self, mock_execute):
        mock_execute.return_value = [{}]
        mock_get_collection = lambda *args: {
            'all_fields': {
                'field': {
                    'type': 'GOB.Reference',
                    'ref': 'dst_catalogue:dst_collection'
                }
            }
        }
        with patch.object(GOBModel, 'get_collection', mock_get_collection):
            result = get_current_relations("catalog", "collection", "field")
            first = next(result)
        mock_execute.assert_called_with("""
SELECT   _gobid, field, _source, _id
FROM     catalog_collection
WHERE    _date_deleted IS NULL
ORDER BY _source, _id
""")

    @patch('gobupload.storage.relate._execute')
    def test_current_relations_with_states(self, mock_execute):
        mock_execute.return_value = [{}]
        mock_get_collection = lambda *args: {
            'all_fields': {
                'field': {
                    'type': 'GOB.Reference',
                    'ref': 'dst_catalogue:dst_collection'
                }
            }
        }
        with patch.object(GOBModel, 'get_collection', mock_get_collection),\
                patch.object(GOBModel, 'has_states', lambda *args: True):
            result = get_current_relations("catalog", "collection", "field")
            first = next(result)
        self.assertEqual(first, {})
        mock_execute.assert_called_with("""
SELECT   _gobid, field, _source, _id, volgnummer, eind_geldigheid
FROM     catalog_collection
WHERE    _date_deleted IS NULL
ORDER BY _source, _id, volgnummer, begin_geldigheid
""")

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
                'destination_attribute': 'dst_attr',
                'method': 'equals'
            }
        ]
        expect = """
SELECT
    CASE
    WHEN src._application = 'src_application' THEN 'equals' END AS method,
    CASE
    WHEN src._application = 'src_application' THEN dst.dst_attr END AS match,
    src._date_deleted AS src__date_deleted,
    src._source AS src__source,
    src._id AS src__id,
    dst._date_deleted AS dst__date_deleted,
    dst._source AS dst__source,
    dst._id AS dst__id,
    dst.dst_attr AS dst_match_dst_attr
FROM catalog_collection AS src
LEFT OUTER JOIN (
SELECT
    _date_deleted,
    _source,
    _id,
    dst_attr
FROM dst_catalogue_dst_collection) AS dst
ON
    (src._application = 'src_application' AND dst.dst_attr = src.src_attr->>'bronwaarde')
WHERE
    (src._application = 'src_application' AND src.src_attr->>'bronwaarde' IS NOT NULL) AND
    (src._date_deleted IS NULL AND dst._date_deleted IS NULL)
ORDER BY
    src._source, src._id
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
                'destination_attribute': 'dst_attr',
                'method': 'equals'
            }
        ]
        expect = """
SELECT
    CASE
    WHEN src._application = 'src_application' THEN 'equals' END AS method,
    CASE
    WHEN src._application = 'src_application' THEN dst.dst_attr END AS match,
    src._date_deleted AS src__date_deleted,
    src._source AS src__source,
    src._id AS src__id,
    src.volgnummer AS src_volgnummer,
    src.begin_geldigheid AS src_begin_geldigheid,
    src.eind_geldigheid AS src_eind_geldigheid,
    dst._date_deleted AS dst__date_deleted,
    dst._source AS dst__source,
    dst._id AS dst__id,
    dst.volgnummer AS dst_volgnummer,
    dst.begin_geldigheid AS dst_begin_geldigheid,
    dst.eind_geldigheid AS dst_eind_geldigheid,
    dst.dst_attr AS dst_match_dst_attr
FROM catalog_collection AS src
LEFT OUTER JOIN (
SELECT
    _date_deleted,
    _source,
    _id,
    volgnummer,
    begin_geldigheid,
    eind_geldigheid,
    dst_attr
FROM dst_catalogue_dst_collection) AS dst
ON
    (src._application = 'src_application' AND dst.dst_attr = src.src_attr->>'bronwaarde') AND
    (dst.begin_geldigheid <= src.eind_geldigheid OR src.eind_geldigheid IS NULL) AND
    (dst.eind_geldigheid >= src.begin_geldigheid OR dst.eind_geldigheid IS NULL)
WHERE
    (src._application = 'src_application' AND src.src_attr->>'bronwaarde' IS NOT NULL) AND
    (src._date_deleted IS NULL AND dst._date_deleted IS NULL)
ORDER BY
    src._source, src._id, src.volgnummer::int, src.begin_geldigheid, dst.begin_geldigheid, dst.eind_geldigheid
"""
        with patch.object(GOBSources, 'get_field_relations', mock_get_field_relations), \
             patch.object(GOBModel, 'get_collection', mock_get_collection), \
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
                'destination_attribute': 'dst_attr',
                'method': 'equals'
            }
        ]
        expect = """
SELECT
    CASE
    WHEN src._application = 'src_application' THEN 'equals' END AS method,
    CASE
    WHEN src._application = 'src_application' THEN dst.dst_attr END AS match,
    src._date_deleted AS src__date_deleted,
    src._source AS src__source,
    src._id AS src__id,
    src.volgnummer AS src_volgnummer,
    src.begin_geldigheid AS src_begin_geldigheid,
    src.eind_geldigheid AS src_eind_geldigheid,
    dst._date_deleted AS dst__date_deleted,
    dst._source AS dst__source,
    dst._id AS dst__id,
    dst.volgnummer AS dst_volgnummer,
    dst.begin_geldigheid AS dst_begin_geldigheid,
    dst.eind_geldigheid AS dst_eind_geldigheid,
    dst.dst_attr AS dst_match_dst_attr
FROM catalog_collection AS src
LEFT OUTER JOIN (
SELECT
    _date_deleted,
    _source,
    _id,
    volgnummer,
    begin_geldigheid,
    eind_geldigheid,
    dst_attr
FROM dst_catalogue_dst_collection) AS dst
ON
    (src._application = 'src_application' AND dst.dst_attr = ANY(ARRAY(SELECT x->>'bronwaarde' FROM jsonb_array_elements(src.src_attr) as x))) AND
    (dst.begin_geldigheid <= src.eind_geldigheid OR src.eind_geldigheid IS NULL) AND
    (dst.eind_geldigheid >= src.begin_geldigheid OR dst.eind_geldigheid IS NULL)
WHERE
    (src._application = 'src_application' AND ARRAY(SELECT x->>'bronwaarde' FROM jsonb_array_elements(src.src_attr) as x) IS NOT NULL) AND
    (src._date_deleted IS NULL AND dst._date_deleted IS NULL)
ORDER BY
    src._source, src._id, src.volgnummer::int, src.begin_geldigheid, dst.begin_geldigheid, dst.eind_geldigheid
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
                'destination_attribute': 'dst_attr1',
                'method': 'equals'
            },
            {
                'source': 'src_application2',
                'field_name': 'src_attr2',
                'destination_attribute': 'dst_attr2',
                'method': 'equals'
            }
        ]
        expect = """
SELECT
    CASE
    WHEN src._application = 'src_application1' THEN 'equals' 
    WHEN src._application = 'src_application2' THEN 'equals' END AS method,
    CASE
    WHEN src._application = 'src_application1' THEN dst.dst_attr1 
    WHEN src._application = 'src_application2' THEN dst.dst_attr2 END AS match,
    src._date_deleted AS src__date_deleted,
    src._source AS src__source,
    src._id AS src__id,
    src.volgnummer AS src_volgnummer,
    src.begin_geldigheid AS src_begin_geldigheid,
    src.eind_geldigheid AS src_eind_geldigheid,
    dst._date_deleted AS dst__date_deleted,
    dst._source AS dst__source,
    dst._id AS dst__id,
    dst.volgnummer AS dst_volgnummer,
    dst.begin_geldigheid AS dst_begin_geldigheid,
    dst.eind_geldigheid AS dst_eind_geldigheid,
    dst.dst_attr1 AS dst_match_dst_attr1,
    dst.dst_attr2 AS dst_match_dst_attr2
FROM catalog_collection AS src
LEFT OUTER JOIN (
SELECT
    _date_deleted,
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
    (dst.begin_geldigheid <= src.eind_geldigheid OR src.eind_geldigheid IS NULL) AND
    (dst.eind_geldigheid >= src.begin_geldigheid OR dst.eind_geldigheid IS NULL)
WHERE
    (src._application = 'src_application1' AND src.src_attr1->>'bronwaarde' IS NOT NULL) OR
    (src._application = 'src_application2' AND src.src_attr2->>'bronwaarde' IS NOT NULL) AND
    (src._date_deleted IS NULL AND dst._date_deleted IS NULL)
ORDER BY
    src._source, src._id, src.volgnummer::int, src.begin_geldigheid, dst.begin_geldigheid, dst.eind_geldigheid
"""
        with patch.object(GOBSources, 'get_field_relations', mock_get_field_relations), \
             patch.object(GOBModel, 'get_collection', mock_get_collection), \
             patch.object(GOBModel, 'has_states', lambda *args: True):
            get_relations("catalog", "collection", "field")
        mock_get_data.assert_called_with(expect)


    @patch('gobupload.storage.relate.GOBStorageHandler', MagicMock())
    def test_get_data(self):
        result = [r for r in _get_data('')]
        self.assertEqual(result, [])

    @patch('gobupload.storage.relate.logger')
    @patch('gobupload.storage.relate._get_data')
    def test_query_missing(self, mock_data, mock_logger):
        mock_data.return_value = []
        _query_missing("any query", "any items name")
        mock_data.assert_called_with("any query")

        mock_logger.warning = mock.MagicMock()
        mock_data.return_value = [{"a": "b"}]
        _query_missing("any query", "any items name")
        mock_logger.warning.assert_called()

    # @patch('gobupload.storage.relate.GOBModel')
    @patch('gobupload.storage.relate._query_missing')
    def test_check_relations(self, mock_missing):
        mock_collection = {
            'all_fields': {
                'any_field_name': {
                    'type': "any type"
                }
            }
        }
        with patch.object(GOBModel, 'get_table_name', lambda s, a, b: a + b), \
             patch.object(GOBModel, 'get_collection', lambda s, a, b: mock_collection), \
             patch.object(GOBModel, 'has_states', lambda s, a, b: True):
            check_relations("any_catalog", "any_collection", "any_field_name")
        mock_missing.assert_called()
        self.assertEqual(mock_missing.call_count, 2)

    def test_update_match(self):
        spec = {
            'method': EQUALS,
            'destination_attribute': "any_dst_attr"
        }
        field = "any_field"
        stmt = _update_match(spec, field, JOIN)
        self.assertEqual(stmt, "dst.any_dst_attr = any_field ->> 'bronwaarde'")

        stmt = _update_match(spec, field, WHERE)
        self.assertEqual(stmt, "any_field IS NOT NULL AND any_field ->> 'bronwaarde' IS NOT NULL")

    @patch('gobupload.storage.relate._geo_resolve', lambda x, y: (x, y))
    def test_update_geo_match(self):
        spec = {
            'method': LIES_IN,
            'destination_attribute': "any_dst_attr"
        }
        field = "any_field"
        s, q = _update_match(spec, field, "any query type")
        self.assertEqual(s, spec)
        self.assertEqual(q, "any query type")

    @patch('gobupload.storage.relate._execute')
    def test_update_relations(self, mock_execute):
        mock._execute = MagicMock()
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
                'destination_attribute': 'dst_attr',
                'method': 'equals'
            }
        ]
        expect = """
UPDATE
    catalog_collection src
SET
    field = new_vals.field_updated
FROM (
    
    SELECT
        src__id, src_volgnummer,
        array_to_json(array_agg(field_updated_elm)) field_updated
    FROM (

        SELECT
            src__id, src_volgnummer,
            src_matchcolumn,
            dst__id, max(dst_volgnummer) dst_volgnummer,
            jsonb_set(
jsonb_set(src_matchcolumn, '{volgnummer}', COALESCE(to_jsonb(max(dst_volgnummer)), 'null'::JSONB))
, '{id}', COALESCE(to_jsonb(dst__id::TEXT), 'null'::JSONB))
                field_updated_elm
        FROM (
SELECT
    CASE
    WHEN src._application = 'src_application' THEN 'equals' END AS method,
    CASE
    WHEN src._application = 'src_application' THEN dst.dst_attr END AS match,
    json_arr_elm AS src_matchcolumn,
    src._date_deleted AS src__date_deleted,
    src._source AS src__source,
    src._id AS src__id,
    src.volgnummer AS src_volgnummer,
    src.begin_geldigheid AS src_begin_geldigheid,
    src.eind_geldigheid AS src_eind_geldigheid,
    dst._date_deleted AS dst__date_deleted,
    dst._source AS dst__source,
    dst._id AS dst__id,
    dst.volgnummer AS dst_volgnummer,
    dst.begin_geldigheid AS dst_begin_geldigheid,
    dst.eind_geldigheid AS dst_eind_geldigheid,
    dst.dst_attr AS dst_match_dst_attr
FROM catalog_collection AS src

JOIN jsonb_array_elements(src.field) AS json_arr_elm ON TRUE

LEFT OUTER JOIN (
SELECT
    _date_deleted,
    _source,
    _id,
    volgnummer,
    begin_geldigheid,
    eind_geldigheid,
    dst_attr
FROM dst_catalogue_dst_collection) AS dst
ON
    (src._application = 'src_application' AND dst.dst_attr = json_arr_elm ->> 'bronwaarde') AND
    (dst.begin_geldigheid <= src.eind_geldigheid OR src.eind_geldigheid IS NULL) AND
    (dst.eind_geldigheid >= src.begin_geldigheid OR dst.eind_geldigheid IS NULL)
WHERE
    (src._application = 'src_application' AND json_arr_elm IS NOT NULL AND json_arr_elm ->> 'bronwaarde' IS NOT NULL) AND
    (src._date_deleted IS NULL AND dst._date_deleted IS NULL)
ORDER BY
    src._source, src._id, src.volgnummer::int, src.begin_geldigheid, dst.begin_geldigheid, dst.eind_geldigheid
) relations
GROUP BY src__id, src_volgnummer, src_matchcolumn, dst__id

    ) last_dsts
    GROUP BY src__id, src_volgnummer

) new_vals
WHERE
    new_vals.src__id = src._id AND new_vals.src_volgnummer = src.volgnummer;
"""
        with patch.object(GOBSources, 'get_field_relations', mock_get_field_relations), \
             patch.object(GOBModel, 'get_collection', mock_get_collection), \
             patch.object(GOBModel, 'has_states', lambda *args: True):
            update_relations("catalog", "collection", "field")
        mock_execute.assert_called_with(expect)

