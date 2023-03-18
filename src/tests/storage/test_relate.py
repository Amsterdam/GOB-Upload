from unittest import TestCase
from unittest.mock import MagicMock, patch, call
from datetime import date, datetime

from gobupload import gob_model
from gobupload.storage.relate import _get_data, get_current_relations, _query_missing
from gobupload.storage.relate import check_relations, check_very_many_relations
from gobupload.storage.relate import check_relation_conflicts, _get_relation_check_query
from gobupload.storage.relate import QA_CHECK, QA_LEVEL, date_to_datetime, _get_date_origin_fields
from gobupload.storage.relate import _convert_row


class TestRelations(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_date_to_datetime(self):
        date_obj = date(2020, 12, 19)
        dt_obj = date_to_datetime(date_obj)

        self.assertEqual('2020-12-19 00:00:00', dt_obj.strftime('%Y-%m-%d %H:%M:%S'))

    def test_get_date_origin_fields(self):
        self.assertEqual([
            "src_begin_geldigheid",
            "dst_begin_geldigheid",
            "src_eind_geldigheid",
            "dst_eind_geldigheid",
        ], _get_date_origin_fields())

    def test_convert_row(self):
        begin = date(2022, 2, 7)
        end = date.today()
        result = _convert_row([("src_begin_geldigheid", begin), ("dst_eind_geldigheid", end)])
        self.assertEqual(result, {"src_begin_geldigheid": begin, "dst_eind_geldigheid": end})

        end = datetime.today()
        result = _convert_row([("src_begin_geldigheid", begin), ("dst_eind_geldigheid", end)])
        self.assertEqual(
            result,
            {"src_begin_geldigheid": date_to_datetime(begin), "dst_eind_geldigheid": end})

    @patch('gobupload.storage.relate._execute')
    def test_current_relations(self, mock_execute):
        mock_execute.return_value = [{}]
        mock_gobmodel_data = {
            'catalog': {
                'collections': {
                    'collection': {
                        'all_fields': {
                            'field': {
                                'type': 'GOB.Reference',
                                'ref': 'dst_catalogue:dst_collection'
                            }
                        }
                    }
                }
            }
        }

        with patch.dict(gob_model.data, mock_gobmodel_data):
            result = get_current_relations("catalog", "collection", "field")
            next(result)
        mock_execute.assert_called_with("""
SELECT   _gobid, field, _source, _id
FROM     catalog_collection
WHERE    _date_deleted IS NULL
ORDER BY _source, _id
""")

    @patch('gobupload.storage.relate._execute')
    def test_current_relations_with_states(self, mock_execute):
        mock_execute.return_value = [{}]
        mock_gobmodel_data = {
            'catalog': {
                'collections': {
                    'collection': {
                        'all_fields': {
                            'field': {
                                'type': 'GOB.Reference',
                                'ref': 'dst_catalogue:dst_collection'
                            }
                        }
                    }
                }
            }
        }

        with patch.object(gob_model, 'has_states', lambda *args: True), \
                patch.dict(gob_model.data, mock_gobmodel_data):
            result = get_current_relations("catalog", "collection", "field")
            first = next(result)
        self.assertEqual(first, {})
        mock_execute.assert_called_with("""
SELECT   _gobid, field, _source, _id, volgnummer, eind_geldigheid
FROM     catalog_collection
WHERE    _date_deleted IS NULL
ORDER BY _source, _id, volgnummer, begin_geldigheid
""")

    @patch('gobupload.storage.relate.GOBStorageHandler', MagicMock(), spec_set=True)
    def test_get_data(self):
        result = list(_get_data(''))
        self.assertEqual(result, [])

    @patch('gobupload.storage.relate.Issue', MagicMock())
    @patch('gobupload.storage.relate.log_issue')
    @patch('gobupload.storage.relate._get_data')
    def test_query_missing(self, mock_data, mock_log_issue):
        mock_data.return_value = []
        _query_missing("any query", {'msg': "any items"}, "name")
        mock_data.assert_called_with("any query")

        mock_data.return_value = [{"a": "b"}]
        _query_missing("any query", {'msg': "any items"}, "name")
        mock_log_issue.assert_called()

    @patch('gobupload.storage.relate._get_relation_check_query')
    @patch('gobupload.storage.relate._query_missing')
    @patch('gobupload.storage.relate.GOBSources.get_field_relations')
    @patch('gobupload.storage.relate.logger')
    def test_check_relations(self, mock_logger, mock_get_field_relations, mock_missing, mock_get_query):
        catalog = 'any_catalog'
        collection = 'any_collection'
        field_name = 'any_field_name'
        mock_get_field_relations.return_value = [{'source': 'sourceA'}]
        with patch.object(gob_model, 'get_table_name', lambda s, a, b: a + b), \
             patch.object(gob_model, 'has_states', lambda s, a, b: True):

            # Test base case: no sources with none_allowed.
            check_relations(catalog, collection, field_name)

            mock_get_query.assert_called()
            mock_get_query.assert_called_with('dangling', catalog, collection, field_name, None)
            self.assertEqual(mock_get_query.call_count, 2)

            mock_missing.assert_called()
            self.assertEqual(mock_missing.call_count, 2)
            mock_logger.info.assert_not_called()

            # Test case: all sources with none_allowed
            mock_get_query.reset_mock()
            mock_missing.reset_mock()
            mock_get_field_relations.return_value = [{'source': 'sourceA', 'none_allowed': True}]

            check_relations(catalog, collection, field_name)

            mock_logger.info.assert_called_once()
            mock_missing.assert_not_called()

            # Test case: hybrid
            mock_get_query.reset_mock()
            mock_missing.reset_mock()
            mock_logger.reset_mock()
            mock_get_field_relations.return_value = [
                {'source': 'sourceA', 'none_allowed': True},
                {'source': 'sourceB', 'none_allowed': False},
                {'source': 'sourceC'},
            ]

            check_relations(catalog, collection, field_name)
            mock_get_query.assert_called_with(
                'dangling', catalog, collection, field_name, ['sourceB', 'sourceC'])

            self.assertEqual(mock_get_query.call_count, 2)

            mock_missing.assert_called()
            self.assertEqual(mock_missing.call_count, 2)
            mock_logger.info.assert_not_called()

    @patch('gobupload.storage.relate.get_relation_name')
    @patch('gobupload.storage.relate._query_missing')
    def test_check_very_many_relations(self, mock_missing, mock_get_relation_name):
        mock_get_relation_name.return_value = 'cat_col_cat2_col2_field'

        with patch.object(gob_model, 'get_table_name', lambda a, b: a + b), \
                patch.object(gob_model, 'has_states', lambda a, b: True):
            check_very_many_relations("any_catalog", "any_collection", "any_field_name")
        mock_missing.assert_called()
        self.assertEqual(mock_missing.call_count, 2)

    @patch("gobupload.storage.relate.GOBStorageHandler")
    @patch('gobupload.storage.relate.Issue')
    @patch('gobupload.storage.relate.log_issue')
    @patch('gobupload.storage.relate.logger')
    @patch('gobupload.storage.relate.Relater')
    def test_check_relation_conflicts(self, mock_relater, mock_logger, mock_log_issue, mock_issue, mock_storage):
        mock_session = MagicMock()
        mock_storage.return_value.get_session.return_value.__enter__.return_value = mock_session

        relater = mock_relater.return_value.__enter__.return_value
        relater.dst_has_states = False

        relater.get_conflicts.return_value = [{
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
        mock_relater.assert_called_with(mock_session, "any_catalog", "any_collection", "any_field_name")
        mock_storage.return_value.get_session.assert_called_with(invalidate=True)

        mock_issue.assert_has_calls([
            call(QA_CHECK.Unique_destination, {
                **relater.get_conflicts.return_value[0],
                'volgnummer': relater.get_conflicts.return_value[0]['src_volgnummer'],
            }, 'src_id', 'bronwaarde'),
            call(QA_CHECK.Unique_destination, {
                **relater.get_conflicts.return_value[1],
                'volgnummer': relater.get_conflicts.return_value[1]['src_volgnummer'],
            }, 'src_id', 'bronwaarde'),
        ])

        self.assertEqual(2, mock_log_issue.call_count)
        mock_log_issue.assert_called_with(mock_logger, QA_LEVEL.WARNING, mock_issue.return_value)

    @patch('gobupload.storage.relate.get_relation_name')
    def test_get_relation_check_query_single(self, mock_get_relation_name):
        mock_gobmodel_data = {
            'any_catalog': {
                'collections': {
                    'any_collection': {
                        'all_fields': {
                            'any_field_name': {
                                'type': "any type"
                            }
                        }
                    }
                }
            }
        }
        mock_get_relation_name.return_value = 'cat_col_cat2_col2_field'

        with patch.object(gob_model, 'get_table_name', lambda a, b: a + b), \
             patch.object(gob_model, 'has_states', lambda a, b: True), \
             patch.dict(gob_model.data, mock_gobmodel_data):

             # Test missing query
            result = _get_relation_check_query(
                "missing", "any_catalog", "any_collection", "any_field_name", [])
            expect = """
SELECT
    src._id as id,
    src._expiration_date,
    src.any_field_name->>'bronwaarde' as bronwaarde,
    src.volgnummer,
    src.begin_geldigheid,
    src.eind_geldigheid
FROM
    any_catalogany_collection src
WHERE
    src._date_deleted IS NULL AND any_field_name->>'bronwaarde' IS NULL
"""
            self.assertEqual(result, expect)

            # Test dangling query
            result = _get_relation_check_query(
                "dangling", "any_catalog", "any_collection", "any_field_name", None)
            expect = """
SELECT
    src._id as id,
    src._expiration_date,
    rel.bronwaarde,
    src.volgnummer,
    src.begin_geldigheid,
    src.eind_geldigheid
FROM
    any_catalogany_collection src
JOIN rel_cat_col_cat2_col2_field rel
ON
    src._id = rel.src_id AND src.volgnummer = rel.src_volgnummer

WHERE
    src._date_deleted IS NULL AND rel.dst_id IS NULL AND rel._date_deleted IS NULL
"""
            self.assertEqual(result, expect)

            # Expect assertionerror on a different query type
            with self.assertRaises(AssertionError):
                result = _get_relation_check_query(
                    "other", "any_catalog", "any_collection", "any_field_name", [])


    @patch('gobupload.storage.relate.get_relation_name')
    def test_get_relation_check_query_many(self, mock_get_relation_name):
        self.maxDiff = None
        mock_gobmodel_data = {
            'any_catalog': {
                'collections': {
                    'any_collection': {
                        'all_fields': {
                            'any_field_name': {
                                'type': "GOB.ManyReference"
                            }
                        }
                    }
                }
            }
        }
        mock_get_relation_name.return_value = 'cat_col_cat2_col2_field'

        with patch.object(gob_model, 'get_table_name', lambda a, b: a + b), \
             patch.object(gob_model, 'has_states', lambda a, b: True), \
             patch.dict(gob_model.data, mock_gobmodel_data):

             # Test missing query
            result = _get_relation_check_query(
                "missing", "any_catalog", "any_collection", "any_field_name", None)
            expect = """
SELECT
    src._id as id,
    src._expiration_date,
    src.any_field_name->>'bronwaarde' as bronwaarde,
    src.volgnummer,
    src.begin_geldigheid,
    src.eind_geldigheid
FROM
    
(
SELECT
    _id,
    _expiration_date,
    _date_deleted,
    jsonb_array_elements(any_field_name) as any_field_name,
    volgnummer,
    begin_geldigheid,
    eind_geldigheid
FROM
    any_catalogany_collection
) AS src

WHERE
    src._date_deleted IS NULL AND any_field_name->>'bronwaarde' IS NULL
"""
            self.assertEqual(result, expect)

            # Test dangling query with applications
            result = _get_relation_check_query(
                "dangling", "any_catalog", "any_collection", "any_field_name", ['applicationA', 'applicationB'])
            expect = """
SELECT
    src._id as id,
    src._expiration_date,
    rel.bronwaarde,
    src.volgnummer,
    src.begin_geldigheid,
    src.eind_geldigheid
FROM
    any_catalogany_collection src
JOIN rel_cat_col_cat2_col2_field rel
ON
    src._id = rel.src_id AND src.volgnummer = rel.src_volgnummer

WHERE
    src._date_deleted IS NULL AND rel.dst_id IS NULL AND rel._date_deleted IS NULL AND (src._application = 'applicationA' OR src._application = 'applicationB')
"""
            self.assertEqual(result, expect)
