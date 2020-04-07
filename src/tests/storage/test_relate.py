from unittest import TestCase, mock
from unittest.mock import MagicMock, patch, call

from gobcore.model import GOBModel
from gobcore.sources import GOBSources

from gobupload.relate.exceptions import RelateException
from gobupload.storage.relate import update_relations, EQUALS, LIES_IN, JOIN, WHERE, _update_match, \
    _get_data, get_last_change, get_current_relations, RelationUpdater, _query_missing, check_relations, \
    check_very_many_relations, _get_updated_row_count, check_relation_conflicts, _get_relation_check_query

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
ORDER BY eventid DESC
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

    @patch('gobupload.storage.relate.GOBStorageHandler', MagicMock())
    def test_get_data(self):
        result = [r for r in _get_data('')]
        self.assertEqual(result, [])

    @patch('gobupload.storage.relate.Issue', mock.MagicMock())
    @patch('gobupload.storage.relate.logger')
    @patch('gobupload.storage.relate.log_issue')
    @patch('gobupload.storage.relate._get_data')
    def test_query_missing(self, mock_data, mock_log_issue, mock_logger):
        mock_data.return_value = []
        _query_missing("any query", {'msg': "any items"}, "name")
        mock_data.assert_called_with("any query")

        mock_data.return_value = [{"a": "b"}]
        _query_missing("any query", {'msg': "any items"}, "name")
        mock_log_issue.assert_called()

    # @patch('gobupload.storage.relate.GOBModel')
    @patch('gobupload.storage.relate._get_relation_check_query')
    @patch('gobupload.storage.relate._query_missing')
    def test_check_relations(self, mock_missing, mock_get_query):
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

        mock_get_query.assert_called()
        self.assertEqual(mock_get_query.call_count, 2)

        mock_missing.assert_called()
        self.assertEqual(mock_missing.call_count, 2)

    @patch('gobupload.storage.relate.get_relation_name')
    @patch('gobupload.storage.relate._query_missing')
    def test_check_very_many_relations(self, mock_missing, mock_get_relation_name):
        mock_collection = {
            'all_fields': {
                'any_field_name': {
                    'type': "any type"
                }
            }
        }

        mock_get_relation_name.return_value = 'cat_col_cat2_col2_field'

        with patch.object(GOBModel, 'get_table_name', lambda s, a, b: a + b), \
             patch.object(GOBModel, 'get_collection', lambda s, a, b: mock_collection), \
             patch.object(GOBModel, 'has_states', lambda s, a, b: True):
            check_very_many_relations("any_catalog", "any_collection", "any_field_name")
        mock_missing.assert_called()
        self.assertEqual(mock_missing.call_count, 2)

    @patch('gobupload.storage.relate.logger')
    @patch('gobupload.storage.relate._execute')
    @patch('gobupload.storage.relate.RelationTableRelater')
    @patch('gobupload.storage.relate._MAX_RELATION_CONFLICTS', 1)
    def test_check_relation_conflicts(self, mock_relater, mock_execute, mock_logger):
        relater = mock_relater.return_value
        relater.dst_has_states = False

        mock_execute.return_value = [{
            'src_id': 1,
            'src_volgnummer': 1,
            'dst_id': 1,
            'bronwaarde': "bronwaarde",
            'row_number': 2
        },
        {
            'src_id': 1,
            'src_volgnummer': 1,
            'dst_id': 2,
            'bronwaarde': "bronwaarde",
            'row_number': 3
        }]

        check_relation_conflicts("any_catalog", "any_collection", "any_field_name")
        mock_relater.assert_called_with("any_catalog", "any_collection", "any_field_name")

        conflicts_msg = f"Conflicting any_field_name relations"
        
        expected = [{
            "id": conflicts_msg,
            "data": {
                "src_id": 1,
                "src_volgnummer": 1,
                "conflict": {
                    "id": 1,
                    "bronwaarde": "bronwaarde"
                }
            }
        },
        {
            "id": conflicts_msg,
            "data": {
                "src_id": 1,
                "src_volgnummer": 1,
                "conflict": {
                    "id": 2,
                    "bronwaarde": "bronwaarde"
                }
            }
        }]

        mock_logger.warning.assert_has_calls([
            call(conflicts_msg, expected[0]),
            call(f"{conflicts_msg}: 2 found, 1 reported"),
        ])



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

    @patch('gobupload.storage.relate.logger', MagicMock())
    @patch('gobupload.storage.relate.get_relation_name')
    @patch('gobupload.storage.relate._execute')
    @patch('gobupload.storage.relate._get_data')
    def test_update_relations(self, mock_get_data, mock_execute, mock_get_relation_name):
        class MockExecute:
            def __init__(self, rowcount):
                self.rowcount = rowcount

        mock_get_relation_name.return_value = 'cat_col_cat2_col2_field'

        mock_execute.return_value = MockExecute(0)
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
                field = new_values.field_updated
            
                FROM (
                    --Select from all changed relations
                    
    SELECT * FROM (
    
    SELECT
        src__id, src_volgnummer,
        to_jsonb(array_to_json(array_agg(src_matchcolumn))) src_matchcolumn,
        to_jsonb(array_to_json(array_agg(field_updated_elm))) field_updated
    FROM (

        SELECT --update specs
            src__id, src_volgnummer,
            dst__id, max(dst_volgnummer) dst_volgnummer,
            src_matchcolumn,
            
            
jsonb_set(
jsonb_set(src_matchcolumn, '{volgnummer}', COALESCE(to_jsonb(max(dst_volgnummer)), 'null'::JSONB))
, '{id}', COALESCE(to_jsonb(dst__id::TEXT), 'null'::JSONB))
    field_updated_elm

        FROM ( --relations
            SELECT
                CASE WHEN src._application = 'src_application' THEN 'equals' END AS method,
                CASE WHEN src._application = 'src_application' THEN dst.dst_attr END AS match,
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
            FROM
                catalog_collection AS src
            
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
                FROM
                    dst_catalogue_dst_collection) AS dst
                ON
                    (src._application = 'src_application' AND dst.dst_attr = json_arr_elm ->> 'bronwaarde') AND
    (dst.begin_geldigheid < src.eind_geldigheid OR src.eind_geldigheid IS NULL) AND
    (dst.eind_geldigheid >= src.eind_geldigheid OR dst.eind_geldigheid IS NULL)
                WHERE
                    (src._application = 'src_application' AND json_arr_elm IS NOT NULL AND json_arr_elm ->> 'bronwaarde' IS NOT NULL) AND (src._date_deleted IS NULL AND dst._date_deleted IS NULL)
                ORDER BY
                    src._source, src._id, src.volgnummer, src.begin_geldigheid, dst.begin_geldigheid, dst.eind_geldigheid
        ) relations
        GROUP BY
            src__id, src_volgnummer,
            src_matchcolumn,
            dst__id
            
    
    ) last_dsts
    GROUP BY
        src__id, src_volgnummer

    ) _outer
    WHERE --only select relations that have changed
        src_matchcolumn != field_updated

                    --Get changed relations in chunks
                    ORDER BY
                        src__id
                    LIMIT
                        50000
                    OFFSET
                        0
                ) new_values
                WHERE
                    new_values.src__id = src._id AND new_values.src_volgnummer = src.volgnummer
        """
        def get_data_values():
            yield {'count': 10}
        mock_get_data.return_value = get_data_values()
        with patch.object(GOBSources, 'get_field_relations', mock_get_field_relations), \
             patch.object(GOBModel, 'get_collection', mock_get_collection), \
             patch.object(GOBModel, 'has_states', lambda *args: True):
            result = update_relations("catalog", "collection", "field")
        mock_execute.assert_called_with(expect)

        self.assertEqual(0, result)

    @patch("gobupload.storage.relate.GOBModel")
    @patch("gobupload.storage.relate.GOBSources")
    def test_update_relations_verymany(self, mock_sources, mock_model):
        model_inst = mock_model.return_value
        mock_src_collection = {'all_fields': MagicMock()}
        model_inst.get_collection.return_value = mock_src_collection
        mock_src_collection['all_fields'].get.return_value = {'type': 'GOB.VeryManyReference'}

        self.assertEqual(0, update_relations('catalog', 'collection', 'fiel'))

        mock_sources.assert_not_called()

    def test_get_updated_row_count(self):
        self.assertEqual(_get_updated_row_count(0, 2), 0)
        self.assertEqual(_get_updated_row_count(1, 2), 1)
        self.assertEqual(_get_updated_row_count(2, 2), 2)

        with self.assertRaises(RelateException):
            _get_updated_row_count(3, 2)

    @patch('gobupload.storage.relate.get_relation_name')
    def test_get_relation_check_query_single(self, mock_get_relation_name):
        mock_collection = {
            'all_fields': {
                'any_field_name': {
                    'type': "any type"
                }
            }
        }

        mock_get_relation_name.return_value = 'cat_col_cat2_col2_field'

        with patch.object(GOBModel, 'get_table_name', lambda s, a, b: a + b), \
             patch.object(GOBModel, 'get_collection', lambda s, a, b: mock_collection), \
             patch.object(GOBModel, 'has_states', lambda s, a, b: True):

             # Test missing query
            result = _get_relation_check_query("missing", "any_catalog", "any_collection", "any_field_name")
            expect = """
SELECT
    src._id as id,
    src.any_field_name->>'bronwaarde' as bronwaarde,
    src._expiration_date,
    src.volgnummer,
    src.begin_geldigheid,
    src.eind_geldigheid
FROM
    any_catalogany_collection AS src
JOIN rel_cat_col_cat2_col2_field rel
ON
    src._id = rel.src_id AND src.volgnummer = rel.src_volgnummer
WHERE
    COALESCE(src._expiration_date, '9999-12-31'::timestamp without time zone) > NOW() AND any_field_name->>'bronwaarde' IS NULL
"""
            self.assertEqual(result, expect)
            
            # Test dangling query
            result = _get_relation_check_query("dangling", "any_catalog", "any_collection", "any_field_name")
            expect = """
SELECT
    src._id as id,
    src.any_field_name->>'bronwaarde' as bronwaarde,
    src._expiration_date,
    src.volgnummer,
    src.begin_geldigheid,
    src.eind_geldigheid
FROM
    any_catalogany_collection AS src
JOIN rel_cat_col_cat2_col2_field rel
ON
    src._id = rel.src_id AND src.volgnummer = rel.src_volgnummer
WHERE
    COALESCE(src._expiration_date, '9999-12-31'::timestamp without time zone) > NOW() AND any_field_name->>'bronwaarde' IS NOT NULL AND rel.dst_id IS NULL
"""
            self.assertEqual(result, expect)

            # Expect assertionerror on a different query type
            with self.assertRaises(AssertionError):
                result = _get_relation_check_query("other", "any_catalog", "any_collection", "any_field_name")


    @patch('gobupload.storage.relate.get_relation_name')
    def test_get_relation_check_query_many(self, mock_get_relation_name):
        self.maxDiff = None
        mock_collection = {
            'all_fields': {
                'any_field_name': {
                    'type': "GOB.ManyReference"
                }
            }
        }

        mock_get_relation_name.return_value = 'cat_col_cat2_col2_field'

        with patch.object(GOBModel, 'get_table_name', lambda s, a, b: a + b), \
             patch.object(GOBModel, 'get_collection', lambda s, a, b: mock_collection), \
             patch.object(GOBModel, 'has_states', lambda s, a, b: True):

             # Test missing query
            result = _get_relation_check_query("missing", "any_catalog", "any_collection", "any_field_name")
            expect = """
SELECT
    src._id as id,
    src.any_field_name->>'bronwaarde' as bronwaarde,
    src._expiration_date,
    src.volgnummer,
    src.begin_geldigheid,
    src.eind_geldigheid
FROM
    
(
SELECT
    _id,
    any_field_name->>'bronwaarde',
    _expiration_date,
    volgnummer,
    begin_geldigheid,
    eind_geldigheid,
    _date_deleted,
    jsonb_array_elements(any_field_name) as any_field_name
FROM
    any_catalogany_collection
) AS src

JOIN rel_cat_col_cat2_col2_field rel
ON
    src._id = rel.src_id AND src.volgnummer = rel.src_volgnummer
WHERE
    COALESCE(src._expiration_date, '9999-12-31'::timestamp without time zone) > NOW() AND any_field_name->>'bronwaarde' IS NULL
"""
            self.assertEqual(result, expect)
            
            # Test dangling query
            result = _get_relation_check_query("dangling", "any_catalog", "any_collection", "any_field_name")
            expect = """
SELECT
    src._id as id,
    src.any_field_name->>'bronwaarde' as bronwaarde,
    src._expiration_date,
    src.volgnummer,
    src.begin_geldigheid,
    src.eind_geldigheid
FROM
    
(
SELECT
    _id,
    any_field_name->>'bronwaarde',
    _expiration_date,
    volgnummer,
    begin_geldigheid,
    eind_geldigheid,
    _date_deleted,
    jsonb_array_elements(any_field_name) as any_field_name
FROM
    any_catalogany_collection
) AS src

JOIN rel_cat_col_cat2_col2_field rel
ON
    src._id = rel.src_id AND src.volgnummer = rel.src_volgnummer
WHERE
    COALESCE(src._expiration_date, '9999-12-31'::timestamp without time zone) > NOW() AND any_field_name->>'bronwaarde' IS NOT NULL AND rel.dst_id IS NULL
"""
            self.assertEqual(result, expect)
