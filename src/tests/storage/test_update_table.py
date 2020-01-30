from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobcore.model.metadata import FIELD
from datetime import date, datetime
from gobupload.storage.update_table import RelationTableUpdater, RelationTableRelater, RelateException


class MockModel:
    _has_states = {
        'src_collection_name': True,
        'dst_collection_name': False,
    }

    def get_collection(self, *args):
        return {
            'all_fields': {
                'src_field_name': {
                    'type': 'GOB.Reference',
                    'ref': 'dst_catalog_name:dst_collection_name',
                }
            }
        }

    def get_table_name(self, catalog_name, collection_name):
        return f"{catalog_name}_{collection_name}_table"

    def has_states(self, catalog_name, collection_name):
        return self._has_states[collection_name]


class MockSources:
    def get_field_relations(self, *args):
        return ['specs']


@patch("gobupload.storage.update_table.RelationTableRelater.model", MockModel())
@patch("gobupload.storage.update_table.RelationTableRelater.sources", MockSources())
@patch("gobupload.storage.update_table.get_relation_name", lambda m, cat, col, field: f"{cat}_{col}_{field}")
class TestRelationTableRelater(TestCase):

    def _get_extractor(self):
        return RelationTableRelater('src_catalog_name', 'src_collection_name', 'src_field_name')

    def test_init(self):
        e = self._get_extractor()

        self.assertEqual('src_catalog_name', e.src_catalog_name)
        self.assertEqual('src_collection_name', e.src_collection_name)
        self.assertEqual('src_field_name', e.src_field_name)
        self.assertEqual(MockModel().get_collection(), e.src_collection)
        self.assertEqual(MockModel().get_collection()['all_fields']['src_field_name'], e.src_field)
        self.assertEqual('dst_catalog_name_dst_collection_name_table', e.dst_table_name)
        self.assertEqual(True, e.src_has_states)
        self.assertEqual(False, e.dst_has_states)
        self.assertEqual(MockSources().get_field_relations(), e.relation_specs)
        self.assertEqual(False, e.is_many)
        self.assertEqual('rel_src_catalog_name_src_collection_name_src_field_name', e.relation_table)

        with patch('gobupload.storage.update_table.RelationTableRelater.sources.get_field_relations',
                   lambda cat, col, field: None):
            with self.assertRaises(RelateException):
                self._get_extractor()

        with patch('gobupload.storage.update_table.RelationTableRelater.model.get_collection',
                   lambda cat, col: {'all_fields': {
                       'src_field_name': {
                           'type': 'GOB.ManyReference',
                           'ref': 'dst_catalog_name:dst_collection_name',
                       }}}):
            e = self._get_extractor()
            self.assertEqual(True, e.is_many)

    def test_select_expressions(self):
        extractor = self._get_extractor()
        extractor._get_derivation = MagicMock(return_value='DERIVATION')
        extractor._get_id = MagicMock(return_value='ID')
        extractor._source_value_ref = MagicMock(return_value='SOURCE_VALUE')
        extractor._validity_select_expressions = MagicMock(return_value=('START_VALIDITY', 'END_VALIDITY'))
        extractor.dst_has_states = False
        extractor.src_has_states = False

        result = extractor._select_expressions()
        minimal = [
            "src._version AS _version",
            "src._application AS _application",
            "src._source_id AS _source_id",
            "'GOB' AS _source",
            "LEAST(src._expiration_date, dst._expiration_date) AS _expiration_date",
            "ID AS id",
            "DERIVATION AS derivation",
            "src._source AS src_source",
            "src._id AS src_id",
            "dst._source AS dst_source",
            "dst._id AS dst_id",
            "SOURCE_VALUE AS bronwaarde",
            "CASE WHEN dst._id IS NULL THEN NULL ELSE START_VALIDITY END AS begin_geldigheid",
            "CASE WHEN dst._id IS NULL THEN NULL ELSE END_VALIDITY END AS eind_geldigheid",
        ]
        src_volgnummer = ["src.volgnummer AS src_volgnummer"]
        dst_volgnumer = ["dst.volgnummer AS dst_volgnummer"]

        expected = minimal
        self.assertEqual(expected, result)

        extractor.dst_has_states = False
        extractor.src_has_states = True

        result = extractor._select_expressions()
        expected = minimal + src_volgnummer
        self.assertEqual(expected, result)

        extractor.dst_has_states = True
        extractor.src_has_states = False

        result = extractor._select_expressions()
        expected = minimal + dst_volgnumer
        self.assertEqual(expected, result)

        extractor.dst_has_states = True
        extractor.src_has_states = True

        result = extractor._select_expressions()
        expected = minimal + src_volgnummer + dst_volgnumer
        self.assertEqual(expected, result)

    def test_get_id(self):
        extractor = self._get_extractor()
        extractor._source_value_ref = MagicMock(return_value='BRONWAARDE')

        no_src_states = "src._id || '.' || src._source || '.' || (BRONWAARDE)"
        with_src_states = "src._id || '.' || src.volgnummer || '.' || src._source || '.' || (BRONWAARDE)"
        extractor.src_has_states = False
        self.assertEqual(no_src_states, extractor._get_id())
        extractor.src_has_states = True
        self.assertEqual(with_src_states, extractor._get_id())

    def test_get_derivation(self):
        extractor = self._get_extractor()
        extractor.relation_specs = [
            {'source': 'A', 'destination_attribute': 'attrA'},
            {'source': 'B', 'destination_attribute': 'attrB'},
        ]

        expected = "CASE src._application\n" \
                   "        WHEN 'A' THEN 'attrA'\n" \
                   "        WHEN 'B' THEN 'attrB'\n" \
                   "    END"
        self.assertEqual(expected, extractor._get_derivation())

    def test_source_value_ref(self):
        extractor = self._get_extractor()
        extractor.is_many = False

        self.assertEqual("src.src_field_name->>'bronwaarde'", extractor._source_value_ref())

        extractor.is_many = True
        self.assertEqual("json_arr_elm.item->>'bronwaarde'", extractor._source_value_ref())

    def test_geo_resolve(self):
        expected = "ST_IsValid(dst.dst_attribute) AND " \
                   "ST_Contains(dst.dst_attribute::geometry, ST_PointOnSurface(src_ref.src_attribute::geometry))"

        extractor = self._get_extractor()

        spec = {
            'destination_attribute': 'dst_attribute',
            'source_attribute': 'src_attribute',
            'method': 'lies_in',
        }
        self.assertEqual(expected, extractor._geo_resolve(spec, 'src_ref'))

    def test_json_obj_ref(self):
        extractor = self._get_extractor()
        extractor.is_many = True
        expected = 'json_arr_elm.item'
        self.assertEqual(expected, extractor._json_obj_ref('src_ref'))

        extractor.is_many = False
        expected = 'src_ref.src_field_name'
        self.assertEqual(expected, extractor._json_obj_ref('src_ref'))

    def test_relate_match(self):
        extractor = self._get_extractor()
        extractor._geo_resolve = MagicMock()
        extractor._json_obj_ref = MagicMock(return_value='json_obj_ref')

        spec = {
            'method': 'equals',
            'destination_attribute': 'dst_attr',
        }
        expected = "dst.dst_attr = json_obj_ref->>'bronwaarde'"
        self.assertEqual(expected, extractor._relate_match(spec, 'src_ref'))

        spec['method'] = 'lies_in'
        self.assertEqual(extractor._geo_resolve.return_value, extractor._relate_match(spec, 'src_ref'))

    def test_dst_table_inner_join_on(self):
        extractor = self._get_extractor()
        extractor.src_has_states = False
        extractor.dst_has_states = False
        extractor.relation_specs = [{'source': 'source1'}, {'source': 'source2'}]
        extractor._relate_match = lambda spec, src_ref: 'relate_match(' + spec['source'] + ')'

        expected = [
            "((src_ref._application = 'source1' AND relate_match(source1)) OR\n"
            "    (src_ref._application = 'source2' AND relate_match(source2)))",
            'dst._date_deleted IS NULL']
        self.assertEqual(expected, extractor._dst_table_inner_join_on('src_ref'))

        extractor.dst_has_states = True
        expected = [
            "((src_ref._application = 'source1' AND relate_match(source1)) OR\n"
            "    (src_ref._application = 'source2' AND relate_match(source2)))",
            '(dst.eind_geldigheid IS NULL OR dst.eind_geldigheid > NOW())',
            'dst._date_deleted IS NULL']
        self.assertEqual(set(expected), set(extractor._dst_table_inner_join_on('src_ref')))

        extractor.src_has_states = True
        expected = [
            "((src_ref._application = 'source1' AND relate_match(source1)) OR\n    "
            "(src_ref._application = 'source2' AND relate_match(source2)))",
            '(dst.begin_geldigheid < src_ref.eind_geldigheid OR src_ref.eind_geldigheid IS NULL)',
            '(dst.eind_geldigheid >= src_ref.eind_geldigheid OR dst.eind_geldigheid IS NULL)',
            'dst._date_deleted IS NULL']
        self.assertEqual(expected, extractor._dst_table_inner_join_on('src_ref'))

    def test_table_outer_join_on(self):
        extractor = self._get_extractor()
        extractor.dst_has_states = False

        expected = [
            'dst._id = src_dst.dst_id',
        ]

        self.assertEqual(expected, extractor._dst_table_outer_join_on())

        extractor.dst_has_states = True
        expected = [
            'dst._id = src_dst.dst_id',
            'dst.volgnummer = src_dst.dst_volgnummer',
        ]

        self.assertEqual(expected, extractor._dst_table_outer_join_on())

    def test_src_dst_join_on(self):
        extractor = self._get_extractor()
        extractor._json_obj_ref = MagicMock(return_value='json_obj_ref')
        extractor.src_has_states = False

        expected = [
            "src_dst.src_id = src._id",
            "src_dst.bronwaarde = json_obj_ref->>'bronwaarde'"
        ]
        self.assertEqual(expected, extractor._src_dst_join_on())

        extractor.src_has_states = True
        expected = [
            "src_dst.src_id = src._id",
            "src_dst.src_volgnummer = src.volgnummer",
            "src_dst.bronwaarde = json_obj_ref->>'bronwaarde'"
        ]
        self.assertEqual(expected, extractor._src_dst_join_on())

    def test_src_dst_select_expressions(self):
        extractor = self._get_extractor()
        extractor.src_has_states = False
        extractor.dst_has_states = False
        extractor._json_obj_ref = MagicMock(return_value='json_obj_ref')

        expected = [
            "src._id AS src_id",
            "dst._id AS dst_id",
            "json_obj_ref->>'bronwaarde' as bronwaarde"
        ]
        self.assertEqual(expected, extractor._src_dst_select_expressions())

        extractor.src_has_states = True
        expected.append("src.volgnummer as src_volgnummer")
        self.assertEqual(expected, extractor._src_dst_select_expressions())

        extractor.dst_has_states = True
        expected.append("max(dst.volgnummer) as dst_volgnummer")
        self.assertEqual(expected, extractor._src_dst_select_expressions())

    def test_src_dst_group_by(self):
        extractor = self._get_extractor()
        extractor.src_has_states = False
        expected = [
            "src._id",
            "dst._id",
            "bronwaarde",
        ]

        self.assertEqual(expected, extractor._src_dst_group_by())

        extractor.src_has_states = True
        expected.append("src.volgnummer")
        self.assertEqual(expected, extractor._src_dst_group_by())

    def test_have_geo_specs(self):
        extractor = self._get_extractor()
        extractor.relation_specs = [{'method': 'equals'}, {'method': 'equals'}]
        self.assertFalse(extractor._have_geo_specs())

        extractor.relation_specs.append({'method': 'lies_in'})
        self.assertTrue(extractor._have_geo_specs())

    def test_valid_geo_src_check(self):
        extractor = self._get_extractor()
        extractor.relation_specs = [
            {'source': 'sourceA', 'source_attribute': 'attrA'},
            {'source': 'sourceB', 'source_attribute': 'attrB'},
        ]
        expected = "(_application = 'sourceA' AND ST_IsValid(attrA)) OR\n    " \
                   "(_application = 'sourceB' AND ST_IsValid(attrB))"
        self.assertEqual(expected, extractor._valid_geo_src_check())

    def test_array_elements(self):
        extractor = self._get_extractor()
        expected = "JOIN jsonb_array_elements(src.src_field_name) json_arr_elm(item) ON TRUE"
        self.assertEqual(expected, extractor._join_array_elements())

    def _get_src_dst_join_mocked_extractor(self):
        extractor = self._get_extractor()
        extractor._valid_geo_src_check = lambda: 'CHECK_VALID_GEO_SRC'
        extractor._src_dst_select_expressions = lambda: ['SRC_DST SELECT EXPRESSION1', 'SRC_DST SELECT EXPRESSION2']
        extractor._join_array_elements = lambda: 'ARRAY_ELEMENTS'
        extractor._dst_table_inner_join_on = lambda: ['DST_TABLE_INNER_JOIN_ON1', 'DST_TABLE_INNER_JOIN_ON2']
        extractor._src_dst_group_by = lambda: ['SRC_DST_GROUP_BY1', 'SRC_DST_GROUP_BY2']
        extractor._src_dst_join_on = lambda: ['SRC_DST_JOIN_ON1', 'SRC_DST_JOIN_ON2']

        return extractor

    def test_src_dst_join_no_geo_singleref(self):
        extractor = self._get_src_dst_join_mocked_extractor()
        extractor._have_geo_specs = lambda: False
        extractor.is_many = False

        expected = """
LEFT JOIN (
    SELECT
        SRC_DST SELECT EXPRESSION1,
    SRC_DST SELECT EXPRESSION2
    FROM (
        SELECT * FROM src_catalog_name_src_collection_name_table WHERE _date_deleted IS NULL
    ) src
    
    
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst ON DST_TABLE_INNER_JOIN_ON1 AND
    DST_TABLE_INNER_JOIN_ON2
    GROUP BY SRC_DST_GROUP_BY1,
    SRC_DST_GROUP_BY2
) src_dst ON SRC_DST_JOIN_ON1 AND
    SRC_DST_JOIN_ON2
"""

        result = extractor._src_dst_join()
        self.assertEqual(expected, result)

    def test_src_dst_join_no_geo_manyref(self):
        extractor = self._get_src_dst_join_mocked_extractor()
        extractor._have_geo_specs = lambda: False
        extractor.is_many = True

        expected = """
LEFT JOIN (
    SELECT
        SRC_DST SELECT EXPRESSION1,
    SRC_DST SELECT EXPRESSION2
    FROM (
        SELECT * FROM src_catalog_name_src_collection_name_table WHERE _date_deleted IS NULL
    ) src
    
    ARRAY_ELEMENTS
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst ON DST_TABLE_INNER_JOIN_ON1 AND
    DST_TABLE_INNER_JOIN_ON2
    GROUP BY SRC_DST_GROUP_BY1,
    SRC_DST_GROUP_BY2
) src_dst ON SRC_DST_JOIN_ON1 AND
    SRC_DST_JOIN_ON2
"""

        result = extractor._src_dst_join()
        self.assertEqual(expected, result)

    def test_src_dst_join_geo_singleref(self):
        extractor = self._get_src_dst_join_mocked_extractor()
        extractor._have_geo_specs = lambda: True
        extractor.is_many = False

        expected = """
LEFT JOIN (
    SELECT
        SRC_DST SELECT EXPRESSION1,
    SRC_DST SELECT EXPRESSION2
    FROM (
        SELECT * FROM src_catalog_name_src_collection_name_table WHERE _date_deleted IS NULL
    ) src
    JOIN (SELECT * FROM src_catalog_name_src_collection_name_table WHERE (CHECK_VALID_GEO_SRC)) valid_src ON src._gobid = valid_src._gobid
    
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst ON DST_TABLE_INNER_JOIN_ON1 AND
    DST_TABLE_INNER_JOIN_ON2
    GROUP BY SRC_DST_GROUP_BY1,
    SRC_DST_GROUP_BY2
) src_dst ON SRC_DST_JOIN_ON1 AND
    SRC_DST_JOIN_ON2
"""

        result = extractor._src_dst_join()
        self.assertEqual(expected, result)

    def test_src_dst_join_geo_manyref(self):
        extractor = self._get_src_dst_join_mocked_extractor()
        extractor._have_geo_specs = lambda: True
        extractor.is_many = True

        expected = """
LEFT JOIN (
    SELECT
        SRC_DST SELECT EXPRESSION1,
    SRC_DST SELECT EXPRESSION2
    FROM (
        SELECT * FROM src_catalog_name_src_collection_name_table WHERE _date_deleted IS NULL
    ) src
    JOIN (SELECT * FROM src_catalog_name_src_collection_name_table WHERE (CHECK_VALID_GEO_SRC)) valid_src ON src._gobid = valid_src._gobid
    ARRAY_ELEMENTS
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst ON DST_TABLE_INNER_JOIN_ON1 AND
    DST_TABLE_INNER_JOIN_ON2
    GROUP BY SRC_DST_GROUP_BY1,
    SRC_DST_GROUP_BY2
) src_dst ON SRC_DST_JOIN_ON1 AND
    SRC_DST_JOIN_ON2
"""

        result = extractor._src_dst_join()
        self.assertEqual(expected, result)

    def test_start_validitity_per_seqnr(self):
        extractor = self._get_extractor()
        expected = """
all_SRC_OR_DST_intervals(
    _id,
    start_volgnummer,
    volgnummer,
    begin_geldigheid,
    eind_geldigheid) AS (
    SELECT
        s._id,
        s.volgnummer,
        s.volgnummer,
        s.begin_geldigheid,
        s.eind_geldigheid
    FROM dst_catalog_name_dst_collection_name_table s
    LEFT JOIN dst_catalog_name_dst_collection_name_table t
    ON s._id = t._id
        AND t.volgnummer::int < s.volgnummer::int
        AND t.eind_geldigheid = s.begin_geldigheid
    WHERE t._id IS NULL
    UNION
    SELECT
        intv._id,
        intv.start_volgnummer,
        SRC_OR_DST.volgnummer,
        intv.begin_geldigheid,
        SRC_OR_DST.eind_geldigheid
    FROM all_SRC_OR_DST_intervals intv
    LEFT JOIN dst_catalog_name_dst_collection_name_table SRC_OR_DST
    ON intv.eind_geldigheid = SRC_OR_DST.begin_geldigheid
        AND SRC_OR_DST._id = intv._id
        AND SRC_OR_DST.volgnummer::int > intv.volgnummer::int
    WHERE SRC_OR_DST.begin_geldigheid IS NOT NULL
), SRC_OR_DST_volgnummer_begin_geldigheid AS (
    SELECT
        _id,
        volgnummer,
        MIN(begin_geldigheid) begin_geldigheid
    FROM all_SRC_OR_DST_intervals
    GROUP BY _id, volgnummer
)"""

        self.assertEqual(expected, extractor._start_validity_per_seqnr('SRC_OR_DST'))

    def test_start_validities(self):
        extractor = self._get_extractor()
        extractor._start_validity_per_seqnr = lambda x: 'VALIDITIES_FOR_' + x.upper()

        extractor.src_has_states = False
        extractor.dst_has_states = False

        self.assertEqual("", extractor._start_validities())

        extractor.src_has_states = True
        self.assertEqual("WITH RECURSIVE VALIDITIES_FOR_SRC", extractor._start_validities())

        extractor.dst_has_states = True
        self.assertEqual("WITH RECURSIVE VALIDITIES_FOR_SRC,VALIDITIES_FOR_DST", extractor._start_validities())

        extractor.src_has_states = False
        self.assertEqual("WITH RECURSIVE VALIDITIES_FOR_DST", extractor._start_validities())

    def test_join_src_geldigheid(self):
        extractor = self._get_extractor()
        extractor.src_has_states = False
        self.assertEqual("", extractor._join_src_geldigheid())
        extractor.src_has_states = True
        self.assertEqual("LEFT JOIN src_volgnummer_begin_geldigheid src_bg "
                         "ON src_bg._id = src._id AND src_bg.volgnummer = src.volgnummer",
                         extractor._join_src_geldigheid())

    def test_join_dst_geldigheid(self):
        extractor = self._get_extractor()
        extractor.dst_has_states = False
        self.assertEqual("", extractor._join_dst_geldigheid())
        extractor.dst_has_states = True
        self.assertEqual("LEFT JOIN dst_volgnummer_begin_geldigheid dst_bg "
                         "ON dst_bg._id = dst._id AND dst_bg.volgnummer = dst.volgnummer",
                         extractor._join_dst_geldigheid())

    def _get_get_query_mocked_extractor(self):
        extractor = self._get_extractor()
        extractor._rel_table_join_on = lambda: ['REL_TABLE_JOIN_ON1', 'REL_TABLE_JOIN_ON2']
        extractor._dst_table_outer_join_on = lambda: ['DST_TABLE_OUTER_JOIN_ON1', 'DST_TABLE_OUTER_JOIN_ON2']
        extractor._select_expressions = lambda: ['SELECT_EXPRESSION1', 'SELECT_EXPRESSION2']
        extractor._where = lambda: 'WHERE CLAUSE'
        extractor._join_array_elements = lambda: 'ARRAY_ELEMENTS'
        extractor._src_dst_join = lambda: 'SRC_DST_JOIN'
        extractor._start_validities = lambda: 'SEQNR_BEGIN_GELDIGHEID'
        extractor._join_src_geldigheid = lambda: 'JOIN_SRC_GELDIGHEID'
        extractor._join_dst_geldigheid = lambda: 'JOIN_DST_GELDIGHEID'

        return extractor

    def test_get_query_singleref(self):
        extractor = self._get_get_query_mocked_extractor()
        extractor.is_many = False

        expected = """
SEQNR_BEGIN_GELDIGHEID
SELECT
    SELECT_EXPRESSION1,
    SELECT_EXPRESSION2
FROM (
    SELECT * FROM src_catalog_name_src_collection_name_table WHERE _date_deleted IS NULL
) src


SRC_DST_JOIN
LEFT JOIN dst_catalog_name_dst_collection_name_table dst
    ON DST_TABLE_OUTER_JOIN_ON1 AND
    DST_TABLE_OUTER_JOIN_ON2
JOIN_SRC_GELDIGHEID
JOIN_DST_GELDIGHEID

WHERE src._date_deleted IS NULL
"""

        result = extractor._get_query()
        self.assertEqual(result, expected)

    def test_get_query_manyref(self):
        extractor = self._get_get_query_mocked_extractor()
        extractor.is_many = True

        expected = """
SEQNR_BEGIN_GELDIGHEID
SELECT
    SELECT_EXPRESSION1,
    SELECT_EXPRESSION2
FROM (
    SELECT * FROM src_catalog_name_src_collection_name_table WHERE _date_deleted IS NULL
) src

ARRAY_ELEMENTS
SRC_DST_JOIN
LEFT JOIN dst_catalog_name_dst_collection_name_table dst
    ON DST_TABLE_OUTER_JOIN_ON1 AND
    DST_TABLE_OUTER_JOIN_ON2
JOIN_SRC_GELDIGHEID
JOIN_DST_GELDIGHEID

WHERE src._date_deleted IS NULL
"""
        result = extractor._get_query()
        self.assertEqual(result, expected)

    @patch("gobupload.storage.update_table._execute")
    def test_extract(self, mock_execute):
        extractor = self._get_extractor()
        extractor._get_query = MagicMock()
        extractor.extract_relations()

        mock_execute.assert_called_with(extractor._get_query.return_value, stream=True)


@patch("gobupload.storage.update_table.RelationTableRelater")
class TestRelationTableUpdater(TestCase):

    def _get_updater(self):
        return RelationTableUpdater('src_catalog_name', 'src_collection_name', 'src_field_name')

    def test_init(self, mock_relater):
        updater = self._get_updater()

        self.assertEqual('src_catalog_name', updater.src_catalog_name)
        self.assertEqual('src_collection_name', updater.src_collection_name)
        self.assertEqual('src_field_name', updater.src_field_name)
        self.assertEqual(mock_relater.return_value, updater.relater)
        self.assertEqual(mock_relater.return_value.relation_table, updater.relation_table)
        self.assertIsNone(updater.filename)

        mock_relater.assert_called_with('src_catalog_name', 'src_collection_name', 'src_field_name')

    def test_format_relation(self, mock_relater):
        rows = [
            {FIELD.START_VALIDITY: None, FIELD.END_VALIDITY: None},
            {'one': 1, 'two': 2},
            {FIELD.START_VALIDITY: date(2020, 1, 30), FIELD.END_VALIDITY: datetime(2020, 2, 27)},
        ]

        expected_result = [
            {FIELD.START_VALIDITY: None, FIELD.END_VALIDITY: None},
            {'one': 1, 'two': 2},
            {FIELD.START_VALIDITY: datetime(2020, 1, 30, 0, 0), FIELD.END_VALIDITY: datetime(2020, 2, 27, 0, 0)},
        ]

        updater = self._get_updater()
        self.assertEqual(expected_result, [updater._format_relation(relation) for relation in rows])

    @patch("gobupload.storage.update_table.ContentsWriter")
    @patch("gobupload.storage.update_table.logger")
    @patch("gobupload.storage.update_table.ProgressTicker", MagicMock())
    def test_update_relation(self, mock_logger, mock_writer, mock_relater):
        updater = self._get_updater()
        updater.relater.extract_relations.return_value = [{'val': i} for i in range(10)]
        updater._format_relation = lambda d: {'formatted': d}

        result = updater.update_relation()
        calls = [call({'formatted': c}) for c in updater.relater.extract_relations.return_value]
        mock_writer_instance = mock_writer.return_value.__enter__.return_value
        mock_writer_instance.write.assert_has_calls(calls)
        mock_logger.info.assert_called_with("Written 10 relations")
        self.assertEqual(mock_writer_instance.filename, result)
