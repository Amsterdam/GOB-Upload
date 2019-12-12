from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobupload.storage.update_table import RelationTableUpdater, RelationTableEventExtractor, RelateException, FIELD


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


@patch("gobupload.storage.update_table.RelationTableEventExtractor.model", MockModel())
@patch("gobupload.storage.update_table.RelationTableEventExtractor.sources", MockSources())
@patch("gobupload.storage.update_table.get_relation_name", lambda m, cat, col, field: f"{cat}_{col}_{field}")
class TestRelationTableEventExtractor(TestCase):

    def _get_extractor(self):
        return RelationTableEventExtractor('src_catalog_name', 'src_collection_name', 'src_field_name')

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

        with patch('gobupload.storage.update_table.RelationTableEventExtractor.sources.get_field_relations',
                   lambda cat, col, field: None):
            with self.assertRaises(RelateException):
                self._get_extractor()

        with patch('gobupload.storage.update_table.RelationTableEventExtractor.model.get_collection',
                   lambda cat, col: {'all_fields': {
                       'src_field_name': {
                           'type': 'GOB.ManyReference',
                           'ref': 'dst_catalog_name:dst_collection_name',
                       }}}):
            e = self._get_extractor()
            self.assertEqual(True, e.is_many)

    def test_select_expressions(self):
        extractor = self._get_extractor()
        extractor.dst_has_states = False
        extractor.src_has_states = False

        result = extractor._select_expressions()
        expected = [
            "src._id AS src__id",
            "src._expiration_date AS src__expiration_date",
            "src.src_field_name->>'bronwaarde' AS src_bronwaarde",
            "src._source AS src__source",
            "src._application AS src__application",
            "src._source_id AS src__source_id",
            "src._version AS src__version",
            "rel._gobid AS rel__gobid",
            "rel.src_id AS rel_src_id",
            "rel.src_volgnummer AS rel_src_volgnummer",
            "rel.dst_id AS rel_dst_id",
            "rel.dst_volgnummer AS rel_dst_volgnummer",
            "rel._expiration_date AS rel__expiration_date",
            "rel._id AS rel__id",
            "CASE WHEN rel._version IS NOT NULL THEN rel._version ELSE '0.1' END AS rel__version",
            "dst._id AS dst__id",
            "dst._expiration_date AS dst__expiration_date",
            "LEAST(src._expiration_date, dst._expiration_date) AS expected_expiration_date",
            "CASE WHEN dst._id IS NULL THEN NULL ELSE NULL END AS expected_begin_geldigheid",
            "CASE WHEN dst._id IS NULL THEN NULL ELSE NULL END AS expected_eind_geldigheid",
            "\n    CASE\n        WHEN rel.src_id IS NULL THEN 'ADD'\n"
            "        WHEN src._id IS NULL THEN 'DELETE'\n"
            "        WHEN dst._id IS DISTINCT FROM rel.dst_id\n             \n"
            "             OR LEAST(src._expiration_date, dst._expiration_date)\n"
            "             IS DISTINCT FROM rel._expiration_date\n"
            "             OR (CASE WHEN dst._id IS NULL THEN NULL ELSE NULL END"
            ")::timestamp without time zone IS DISTINCT FROM rel.begin_geldigheid\n"
            "             OR (CASE WHEN dst._id IS NULL THEN NULL ELSE NULL END"
            ")::timestamp without time zone IS DISTINCT FROM rel.eind_geldigheid\n"
            "             THEN 'MODIFY'\n        ELSE 'CONFIRM'\n    END AS event_type"
        ]
        self.assertEqual(expected, result)

        extractor.src_has_states = True
        expected = [
            "src._id AS src__id",
            "src._expiration_date AS src__expiration_date",
            "src.src_field_name->>'bronwaarde' AS src_bronwaarde",
            "src._source AS src__source",
            "src._application AS src__application",
            "src._source_id AS src__source_id",
            "src._version AS src__version",
            "rel._gobid AS rel__gobid",
            "rel.src_id AS rel_src_id",
            "rel.src_volgnummer AS rel_src_volgnummer",
            "rel.dst_id AS rel_dst_id",
            "rel.dst_volgnummer AS rel_dst_volgnummer",
            "rel._expiration_date AS rel__expiration_date",
            "rel._id AS rel__id",
            "CASE WHEN rel._version IS NOT NULL THEN rel._version ELSE '0.1' END AS rel__version",
            "dst._id AS dst__id",
            "dst._expiration_date AS dst__expiration_date",
            "LEAST(src._expiration_date, dst._expiration_date) AS expected_expiration_date",
            "CASE WHEN dst._id IS NULL THEN NULL ELSE src_bg.begin_geldigheid END AS expected_begin_geldigheid",
            "CASE WHEN dst._id IS NULL THEN NULL ELSE src.eind_geldigheid END AS expected_eind_geldigheid",
            "\n    CASE\n        WHEN rel.src_id IS NULL THEN 'ADD'\n"
            "        WHEN src._id IS NULL THEN 'DELETE'\n"
            "        WHEN dst._id IS DISTINCT FROM rel.dst_id\n             \n"
            "             OR LEAST(src._expiration_date, dst._expiration_date)\n"
            "             IS DISTINCT FROM rel._expiration_date\n"
            "             OR (CASE WHEN dst._id IS NULL THEN NULL ELSE src_bg.begin_geldigheid END"
            ")::timestamp without time zone IS DISTINCT FROM rel.begin_geldigheid\n"
            "             OR (CASE WHEN dst._id IS NULL THEN NULL ELSE src.eind_geldigheid END"
            ")::timestamp without time zone IS DISTINCT FROM rel.eind_geldigheid\n"
            "             THEN 'MODIFY'\n        ELSE 'CONFIRM'\n    END AS event_type",
            "src.volgnummer AS src_volgnummer",
        ]
        result = extractor._select_expressions()
        self.assertEqual(set(expected), set(result))

        expected = [
            "src._id AS src__id",
            "src._expiration_date AS src__expiration_date",
            "src.src_field_name->>'bronwaarde' AS src_bronwaarde",
            "src._source AS src__source",
            "src._application AS src__application",
            "src._source_id AS src__source_id",
            "src._version AS src__version",
            "rel._gobid AS rel__gobid",
            "rel.src_id AS rel_src_id",
            "rel.src_volgnummer AS rel_src_volgnummer",
            "rel.dst_id AS rel_dst_id",
            "rel.dst_volgnummer AS rel_dst_volgnummer",
            "rel._expiration_date AS rel__expiration_date",
            "rel._id AS rel__id",
            "CASE WHEN rel._version IS NOT NULL THEN rel._version ELSE \'0.1\' END AS rel__version",
            "dst._id AS dst__id",
            "dst._expiration_date AS dst__expiration_date",
            "LEAST(src._expiration_date, dst._expiration_date) AS expected_expiration_date",
            "CASE WHEN dst._id IS NULL THEN NULL ELSE GREATEST(src_bg.begin_geldigheid, dst_bg.begin_geldigheid) "
            "END AS expected_begin_geldigheid",
            "CASE WHEN dst._id IS NULL THEN NULL ELSE LEAST(src.eind_geldigheid, dst.eind_geldigheid) "
            "END AS expected_eind_geldigheid",
            "\n    CASE\n        WHEN rel.src_id IS NULL THEN 'ADD'\n"
            "        WHEN src._id IS NULL THEN 'DELETE'\n"
            "        WHEN dst._id IS DISTINCT FROM rel.dst_id\n"
            "             OR dst.volgnummer IS DISTINCT FROM rel.dst_volgnummer\n"
            "             OR LEAST(src._expiration_date, dst._expiration_date)\n"
            "             IS DISTINCT FROM rel._expiration_date\n"
            "             OR (CASE WHEN dst._id IS NULL THEN NULL ELSE "
            "GREATEST(src_bg.begin_geldigheid, dst_bg.begin_geldigheid) END"
            ")::timestamp without time zone IS DISTINCT FROM rel.begin_geldigheid\n"
            "             OR (CASE WHEN dst._id IS NULL THEN NULL ELSE "
            "LEAST(src.eind_geldigheid, dst.eind_geldigheid) END"
            ")::timestamp without time zone IS DISTINCT FROM rel.eind_geldigheid\n"
            "             THEN 'MODIFY'\n"
            "        ELSE 'CONFIRM'\n    END AS event_type",
            "src.volgnummer AS src_volgnummer",
            "dst.volgnummer AS dst_volgnummer",
        ]
        extractor.dst_has_states = True
        result = extractor._select_expressions()
        self.assertEqual(set(expected), set(result))
        self.maxDiff = None
        extractor.src_has_states = False
        expected = [
            "src._id AS src__id",
            "src._expiration_date AS src__expiration_date",
            "src.src_field_name->>'bronwaarde' AS src_bronwaarde",
            "src._source AS src__source",
            "src._application AS src__application",
            "src._source_id AS src__source_id",
            "src._version AS src__version",
            "rel._gobid AS rel__gobid",
            "rel.src_id AS rel_src_id",
            "rel.src_volgnummer AS rel_src_volgnummer",
            "rel.dst_id AS rel_dst_id",
            "rel.dst_volgnummer AS rel_dst_volgnummer",
            "rel._expiration_date AS rel__expiration_date",
            "rel._id AS rel__id",
            "CASE WHEN rel._version IS NOT NULL THEN rel._version ELSE '0.1' END AS rel__version",
            "dst._id AS dst__id",
            "dst._expiration_date AS dst__expiration_date",
            "LEAST(src._expiration_date, dst._expiration_date) AS expected_expiration_date",
            "CASE WHEN dst._id IS NULL THEN NULL ELSE dst_bg.begin_geldigheid END AS expected_begin_geldigheid",
            "CASE WHEN dst._id IS NULL THEN NULL ELSE dst.eind_geldigheid END AS expected_eind_geldigheid",
            "\n    CASE\n        WHEN rel.src_id IS NULL THEN 'ADD'\n"
            "        WHEN src._id IS NULL THEN 'DELETE'\n"
            "        WHEN dst._id IS DISTINCT FROM rel.dst_id\n"
            "             OR dst.volgnummer IS DISTINCT FROM rel.dst_volgnummer\n"
            "             OR LEAST(src._expiration_date, dst._expiration_date)\n"
            "             IS DISTINCT FROM rel._expiration_date\n"
            "             OR (CASE WHEN dst._id IS NULL THEN NULL ELSE dst_bg.begin_geldigheid END"
            ")::timestamp without time zone IS DISTINCT FROM rel.begin_geldigheid\n"
            "             OR (CASE WHEN dst._id IS NULL THEN NULL ELSE dst.eind_geldigheid END"
            ")::timestamp without time zone IS DISTINCT FROM rel.eind_geldigheid\n"
            "             THEN 'MODIFY'\n        ELSE 'CONFIRM'\n    END AS event_type",
            "dst.volgnummer AS dst_volgnummer",
        ]
        result = extractor._select_expressions()
        self.assertEqual(set(expected), set(result))

    def test_source_value_ref(self):
        extractor = self._get_extractor()
        extractor.is_many = False

        self.assertEqual("src.src_field_name->>'bronwaarde'", extractor._source_value_ref())

        extractor.is_many = True
        self.assertEqual("json_arr_elm.item->>'bronwaarde'", extractor._source_value_ref())

    def test_rel_table_join_on(self):
        extractor = self._get_extractor()
        extractor.src_has_states = False

        expected = ['src._id = rel.src_id', 'rel.src_source = src._source', 'rel._date_deleted IS NULL',
                    "rel.bronwaarde = src.src_field_name->>'bronwaarde'", 'rel._application = src._application']

        self.assertEqual(expected, extractor._rel_table_join_on())

        extractor.src_has_states = True
        expected.append('src.volgnummer = rel.src_volgnummer')

        self.assertEqual(set(expected), set(extractor._rel_table_join_on()))

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

    def test_where(self):
        extractor = self._get_extractor()
        extractor._source_value_ref = lambda: 'source_value_ref'

        expected = 'WHERE NOT (src._id IS NOT NULL AND source_value_ref IS NULL AND rel._id IS NULL) ' \
                   'AND rel._date_deleted IS NULL'
        self.assertEqual(expected, extractor._where())

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


FULL JOIN rel_src_catalog_name_src_collection_name_src_field_name rel
    ON REL_TABLE_JOIN_ON1 AND
    REL_TABLE_JOIN_ON2
SRC_DST_JOIN
LEFT JOIN dst_catalog_name_dst_collection_name_table dst
    ON DST_TABLE_OUTER_JOIN_ON1 AND
    DST_TABLE_OUTER_JOIN_ON2
JOIN_SRC_GELDIGHEID
JOIN_DST_GELDIGHEID

WHERE CLAUSE
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
FULL JOIN rel_src_catalog_name_src_collection_name_src_field_name rel
    ON REL_TABLE_JOIN_ON1 AND
    REL_TABLE_JOIN_ON2
SRC_DST_JOIN
LEFT JOIN dst_catalog_name_dst_collection_name_table dst
    ON DST_TABLE_OUTER_JOIN_ON1 AND
    DST_TABLE_OUTER_JOIN_ON2
JOIN_SRC_GELDIGHEID
JOIN_DST_GELDIGHEID

WHERE CLAUSE
"""
        result = extractor._get_query()
        self.assertEqual(result, expected)

    @patch("gobupload.storage.update_table._execute")
    def test_extract(self, mock_execute):
        extractor = self._get_extractor()
        extractor._get_query = MagicMock(return_value='THE QUERY')
        extractor.extract()

        mock_execute.assert_called_with(f"SELECT * FROM (THE QUERY) q WHERE event_type <> 'CONFIRM'", stream=True)


@patch("gobupload.storage.update_table.RelationTableEventExtractor")
class TestRelationTableUpdater(TestCase):

    def _get_updater(self):
        return RelationTableUpdater('src_catalog_name', 'src_collection_name', 'src_field_name')

    @patch("gobupload.storage.update_table.RelationTableUpdater._get_fields")
    def test_init(self, mock_get_fields, mock_event_extractor):
        updater = self._get_updater()

        self.assertEqual('src_catalog_name', updater.src_catalog_name)
        self.assertEqual('src_collection_name', updater.src_collection_name)
        self.assertEqual('src_field_name', updater.src_field_name)
        self.assertEqual(mock_event_extractor.return_value, updater.events_extractor)
        self.assertEqual(mock_event_extractor.return_value.relation_table, updater.relation_table)
        self.assertEqual(mock_get_fields.return_value, updater.fields)
        self.assertEqual(0, updater.update_cnt)

        mock_event_extractor.assert_called_with('src_catalog_name', 'src_collection_name', 'src_field_name')

    def test_get_fields(self, mock_event_extractor):
        updater = self._get_updater()
        updater.events_extractor.src_has_states = False
        updater.events_extractor.dst_has_states = False

        expected_result = {
            '_gobid': 'rel__gobid',
            '_id': None,
            '_source': 'src__source',
            '_application': 'src__application',
            '_source_id': 'src__source_id',
            '_version': 'rel__version',
            'begin_geldigheid': 'expected_begin_geldigheid',
            'eind_geldigheid': 'expected_eind_geldigheid',
            'id': None,
            'src_source': 'src__source',
            'src_id': 'src__id',
            'dst_source': 'dst__source',
            'dst_id': 'dst__id',
            '_expiration_date': 'expected_expiration_date',
            'bronwaarde': 'src_bronwaarde'
        }
        self.assertEqual(expected_result, updater._get_fields())

        updater.events_extractor.src_has_states = True
        expected_result['src_volgnummer'] = 'src_volgnummer'
        self.assertEqual(expected_result, updater._get_fields())

        updater.events_extractor.dst_has_states = True
        expected_result['dst_volgnummer'] = 'dst_volgnummer'
        self.assertEqual(expected_result, updater._get_fields())

    def test_values_list(self, mock_event_extractor):
        updater = self._get_updater()
        updater.fields = {
            FIELD.GOBID: 'gobid_key',
            'other_field': 'other_key',
            'other_field2': 'other_key2',
            'none_field': None,
        }

        event = {
            'gobid_key': 4294,
            'other_key': 'other_value',
        }

        expected_result = ["'other_value'", "NULL", "NULL"]
        self.assertEqual(expected_result, updater._values_list(event, False))

        expected_result = ["4294", "'other_value'", "NULL", "NULL"]
        self.assertEqual(expected_result, updater._values_list(event, True))

    def test_column_list(self, mock_event_extractor):
        updater = self._get_updater()
        updater.fields = {
            FIELD.GOBID: 'gobid_key',
            'other_field': 'other_key',
            'other_field2': 'other_key2',
            'none_field': None,
        }

        self.assertEqual('other_field,other_field2,none_field', updater._column_list())
        self.assertEqual('_gobid,other_field,other_field2,none_field', updater._column_list(True))

    def test_write_events(self, mock_event_extractor):
        updater = self._get_updater()
        updater._write_add_events = MagicMock()
        updater._write_modify_events = MagicMock()
        updater._write_delete_events = MagicMock()
        queue = []

        updater._write_events(queue)
        self.assertEqual(0, updater.update_cnt)

        queue = [{'event_type': 'ADD'}]
        updater._write_events(queue)
        self.assertEqual(1, updater.update_cnt)
        self.assertEqual(0, len(queue), "Queue should be emptied")
        updater._write_add_events.assert_called_with(queue)

        queue = [{'event_type': 'MODIFY'}, {}, {}, {}]
        updater._write_events(queue)
        self.assertEqual(5, updater.update_cnt)
        self.assertEqual(0, len(queue), "Queue should be emptied")
        updater._write_modify_events.assert_called_with(queue)

        queue = [{'event_type': 'DELETE'}, {}, {}, {}, {}, {}]
        updater._write_events(queue)
        self.assertEqual(11, updater.update_cnt)
        self.assertEqual(0, len(queue), "Queue should be emptied")
        updater._write_modify_events.assert_called_with(queue)

    @patch('gobupload.storage.update_table._execute')
    def test_write_add_events(self, mock_execute, mock_event_extractor):
        updater = self._get_updater()
        updater._values_list = lambda x: ['VALUES_LIST_' + x['name']]
        updater.relation_table = 'RELATION_TABLE'
        updater.column_list = lambda: 'COLUMN_LIST'

        events = [{'name': 'EVENT1'}, {'name': 'EVENT2'}]
        updater._write_add_events(events)

        expected_query = 'INSERT INTO RELATION_TABLE (_id,_source,_application,_source_id,_version,id,' \
                         'src_source,src_id,dst_source,dst_id,begin_geldigheid,eind_geldigheid,_expiration_date,' \
                         'bronwaarde,src_volgnummer,' \
                         'dst_volgnummer' \
                         ') VALUES (VALUES_LIST_EVENT1),\n(VALUES_LIST_EVENT2)'

        mock_execute.assert_called_with(expected_query)

    @patch('gobupload.storage.update_table._execute')
    def test_write_modify_events(self, mock_execute, mock_event_extractor):
        updater = self._get_updater()
        updater._values_list = lambda x, y: ['VALUES_LIST_' + x['name']]
        updater.relation_table = 'RELATION_TABLE'
        updater.column_list = lambda x: 'COLUMN_LIST'
        updater.fields = {
            'fieldA': 'valA',
            'fieldB': 'valB',
            '_expiration_date': 'valC',
        }

        events = [{'name': 'EVENT1'}, {'name': 'EVENT2'}]

        updater._write_modify_events(events)

        expected_query = 'UPDATE RELATION_TABLE AS rel\n' \
                         'SET fieldA = v.fieldA,\n' \
                         'fieldB = v.fieldB,\n' \
                         '_expiration_date = v._expiration_date::timestamp without time zone\n' \
                         'FROM (VALUES (VALUES_LIST_EVENT1),\n(VALUES_LIST_EVENT2)) ' \
                         'AS v(fieldA,fieldB,_expiration_date)\n' \
                         'WHERE rel._gobid = v._gobid'

        mock_execute.assert_called_with(expected_query)

    @patch('gobupload.storage.update_table._execute')
    def test_write_delete_events(self, mock_execute, mock_event_extractor):
        updater = self._get_updater()
        updater.relation_table = 'RELATION_TABLE'
        events = [{'rel__gobid': 1}, {'rel__gobid': 2}, {'rel__gobid': 3}]

        updater._write_delete_events(events)

        expected_query = 'UPDATE RELATION_TABLE\n' \
                         'SET _date_deleted = NOW()\n' \
                         'WHERE _gobid IN (1,2,3)'

        mock_execute.assert_called_with(expected_query)

    def test_add_event_to_queue(self, mock_event_extractor):
        updater = self._get_updater()
        updater._write_events = MagicMock()
        updater.MAX_QUEUE_LENGTH = 3

        queue = []

        updater._add_event_to_queue({}, queue)
        self.assertEqual(1, len(queue))
        updater._write_events.assert_not_called()

        updater._add_event_to_queue({}, queue)
        self.assertEqual(2, len(queue))
        updater._write_events.assert_not_called()

        updater._add_event_to_queue({}, queue)
        self.assertEqual(3, len(queue))
        updater._write_events.assert_called_with(queue)

    def test_update_relation(self, mock_event_extractor):
        updater = self._get_updater()
        updater._write_events = MagicMock()
        updater._refresh_materialized_view = MagicMock()
        updater.update_cnt = 240
        updater.events_extractor.extract.return_value = [
            {'event_type': 'ADD'},
            {'event_type': 'MODIFY'},
            {'event_type': 'DELETE'},
            {'event_type': 'DELETE'},
            {'event_type': 'MODIFY'},
            {'event_type': 'MODIFY'},
        ]

        result = updater.update_relation()

        updater._write_events.assert_has_calls([
            call([{'event_type': 'ADD'}]),
            call([{'event_type': 'MODIFY'}, {'event_type': 'MODIFY'}, {'event_type': 'MODIFY'}]),
            call([{'event_type': 'DELETE'}, {'event_type': 'DELETE'}]),
        ])
        updater._refresh_materialized_view.assert_called_once()

        self.assertEqual(240, result)

    @patch("gobupload.storage.update_table.GOBStorageHandler")
    @patch("gobupload.storage.update_table.MaterializedViews")
    def test_refresh_materialized_view(self, mock_materialized_views, mock_storage_handler, mock_event_extractor):
        mock_mv_instance = MagicMock()
        mock_materialized_views.return_value.get.return_value = mock_mv_instance

        updater = self._get_updater()
        updater._refresh_materialized_view()

        mock_materialized_views.return_value.get.assert_called_with(updater.src_catalog_name,
                                                                    updater.src_collection_name,
                                                                    updater.src_field_name)
        mock_mv_instance.refresh.assert_called_with(mock_storage_handler.return_value)


class CheckerMockModel:
    _collections = {
        'collection_a': {
            'has_states': 'some boolean value',
            'all_fields': {
                'field_name': {
                    'type': 'GOB.ManyReference',
                },
                'string_field': {
                    'type': 'GOB.String',
                }
            },
            'attributes': 'collection_a attributes',
        },
        'collection_b': {
            'has_states': 'some boolean value',
            'all_fields': {
                'field_name': {
                    'type': 'GOB.Reference',
                }
            },
            'attributes': 'collection_b attributes',
        }

    }

    def get_table_name(self, catalog_name, collection_name):
        return f"{catalog_name}_{collection_name}"

    def has_states(self, catalog_name, collection_name):
        return self._collections[collection_name]['has_states']

    def get_collection(self, catalog_name, collection_name):
        if catalog_name == 'the catalog':
            return self._collections[collection_name]
        return {}

    def get_catalogs(self):
        return ['catalog a', 'catalog b']

    def get_collections(self, catalog_name):
        if catalog_name == 'the catalog':
            # Only return for the catalog
            return self._collections.keys()
        return {}

    def _extract_references(self, attributes):
        if attributes == 'collection_a attributes':
            return {'reference a': 'ref', 'reference b': 'ref'}
        return {}
