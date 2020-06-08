from unittest import TestCase, mock
from unittest.mock import MagicMock, patch, call

from gobcore.model import GOBModel

from gobupload.relate.exceptions import RelateException
from gobupload.storage.relate import EQUALS, LIES_IN, JOIN, WHERE, \
    _get_data, get_current_relations, _query_missing, check_relations, \
    check_very_many_relations, check_relation_conflicts, _get_relation_check_query, QA_CHECK, QA_LEVEL


class TestRelations(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

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

    @patch('gobupload.storage.relate.Issue')
    @patch('gobupload.storage.relate.log_issue')
    @patch('gobupload.storage.relate.logger')
    @patch('gobupload.storage.relate._execute')
    @patch('gobupload.storage.relate.Relater')
    @patch('gobupload.storage.relate._MAX_RELATION_CONFLICTS', 1)
    def test_check_relation_conflicts(self, mock_relater, mock_execute, mock_logger, mock_log_issue, mock_issue):
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

        mock_issue.assert_has_calls([
            call(QA_CHECK.Unique_destination, {
                **mock_execute.return_value[0],
                'volgnummer': mock_execute.return_value[0]['src_volgnummer'],
            }, 'src_id', 'bronwaarde'),
        ])

        mock_log_issue.assert_called_with(mock_logger, QA_LEVEL.WARNING, mock_issue.return_value)

        mock_logger.data_warning.assert_has_calls([
            call(f"{conflicts_msg}: 2 found, 1 reported"),
        ])

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
