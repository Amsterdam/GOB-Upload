from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobcore.model.metadata import FIELD
from datetime import date, datetime
from gobupload.relate.table.update_table import RelationTableRelater, RelateException


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


@patch("gobupload.relate.table.update_table.RelationTableRelater.model", MockModel())
@patch("gobupload.relate.table.update_table.RelationTableRelater.sources", MockSources())
@patch("gobupload.relate.table.update_table.get_relation_name", lambda m, cat, col, field: f"{cat}_{col}_{field}")
class TestRelationTableRelater(TestCase):

    def _get_relater(self):
        return RelationTableRelater('src_catalog_name', 'src_collection_name', 'src_field_name')

    def test_init(self):
        e = self._get_relater()

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

        with patch('gobupload.relate.table.update_table.RelationTableRelater.sources.get_field_relations',
                   lambda cat, col, field: None):
            with self.assertRaises(RelateException):
                self._get_relater()

        with patch('gobupload.relate.table.update_table.RelationTableRelater.model.get_collection',
                   lambda cat, col: {'all_fields': {
                       'src_field_name': {
                           'type': 'GOB.ManyReference',
                           'ref': 'dst_catalog_name:dst_collection_name',
                       }}}):
            e = self._get_relater()
            self.assertEqual(True, e.is_many)

    def test_validity_select_expressions(self):
        test_cases = [
            (True, True, ('GREATEST(src_bg.begin_geldigheid, dst_bg.begin_geldigheid)',
                          'LEAST(src.eind_geldigheid, dst.eind_geldigheid)')),
            (True, False, ('src_bg.begin_geldigheid', 'src.eind_geldigheid')),
            (False, True, ('dst_bg.begin_geldigheid', 'dst.eind_geldigheid')),
            (False, False, ('NULL', 'NULL')),
        ]

        relater = self._get_relater()

        for src_has_states, dst_has_states, result in test_cases:
            relater.src_has_states = src_has_states
            relater.dst_has_states = dst_has_states
            self.assertEqual(result, relater._validity_select_expressions())

    def test_build_select_expressions(self):
        relater = self._get_relater()
        relater.src_has_states = False
        relater.dst_has_states = False
        relater.select_aliases = ['a', 'b']

        mapping = {'a': 'some a', 'b': 'some b'}
        self.assertEqual([
            'some a AS a',
            'some b AS b',
        ], relater._build_select_expressions(mapping))

        relater.src_has_states = True
        relater.dst_has_states = True
        mapping.update({
            'src_volgnummer': 'some src volgnummer',
            'dst_volgnummer': 'some dst volgnummer',
        })

        self.assertEqual([
            'some a AS a',
            'some b AS b',
            'some src volgnummer AS src_volgnummer',
            'some dst volgnummer AS dst_volgnummer',
        ], relater._build_select_expressions(mapping))

        # All keys must be present
        keys = mapping.keys()
        for key in keys:
            cloned_mapping = mapping.copy()
            del cloned_mapping[key]

            with self.assertRaises(AssertionError):
                relater._build_select_expressions(cloned_mapping)

    def test_select_expressions_src(self):
        relater = self._get_relater()
        relater._get_id = lambda: 'ID'
        relater._get_derivation = lambda: 'DERIVATION'
        relater._source_value_ref = lambda: 'SOURCE VALUE'
        relater._build_select_expressions = MagicMock()
        relater._validity_select_expressions = lambda: ('START_VALIDITY', 'END_VALIDITY')
        result = relater._select_expressions_src()
        self.assertEqual(relater._build_select_expressions.return_value, result)

    def test_select_expressions_dst(self):
        relater = self._get_relater()
        relater._get_id = lambda: 'ID'
        relater._get_derivation = lambda: 'DERIVATION'
        relater._source_value_ref = lambda: 'SOURCE VALUE'
        relater._build_select_expressions = MagicMock()
        relater._validity_select_expressions = lambda: ('START_VALIDITY', 'END_VALIDITY')
        result = relater._select_expressions_dst()
        self.assertEqual(relater._build_select_expressions.return_value, result)

    def test_get_id(self):
        relater = self._get_relater()
        relater._source_value_ref = MagicMock(return_value='BRONWAARDE')

        no_src_states = "src._id || '.' || src._source || '.' || (BRONWAARDE)"
        with_src_states = "src._id || '.' || src.volgnummer || '.' || src._source || '.' || (BRONWAARDE)"
        relater.src_has_states = False
        self.assertEqual(no_src_states, relater._get_id())
        relater.src_has_states = True
        self.assertEqual(with_src_states, relater._get_id())

    def test_get_derivation(self):
        relater = self._get_relater()
        relater.relation_specs = [
            {'source': 'A', 'destination_attribute': 'attrA'},
            {'source': 'B', 'destination_attribute': 'attrB'},
        ]

        expected = "CASE src._application\n" \
                   "        WHEN 'A' THEN 'attrA'\n" \
                   "        WHEN 'B' THEN 'attrB'\n" \
                   "    END"
        self.assertEqual(expected, relater._get_derivation())

    def test_source_value_ref(self):
        relater = self._get_relater()
        relater.is_many = False

        self.assertEqual("src.src_field_name->>'bronwaarde'", relater._source_value_ref())

        relater.is_many = True
        self.assertEqual("json_arr_elm.item->>'bronwaarde'", relater._source_value_ref())

    def test_geo_resolve(self):
        expected = "ST_IsValid(dst.dst_attribute) AND " \
                   "ST_Contains(dst.dst_attribute::geometry, ST_PointOnSurface(src_ref.src_attribute::geometry))"

        relater = self._get_relater()

        spec = {
            'destination_attribute': 'dst_attribute',
            'source_attribute': 'src_attribute',
            'method': 'lies_in',
        }
        self.assertEqual(expected, relater._geo_resolve(spec, 'src_ref'))

    def test_json_obj_ref(self):
        relater = self._get_relater()
        relater.is_many = True
        expected = 'json_arr_elm.item'
        self.assertEqual(expected, relater._json_obj_ref('src_ref'))

        relater.is_many = False
        expected = 'src_ref.src_field_name'
        self.assertEqual(expected, relater._json_obj_ref('src_ref'))

    def test_relate_match(self):
        relater = self._get_relater()
        relater._geo_resolve = MagicMock()
        relater._json_obj_ref = MagicMock(return_value='json_obj_ref')

        spec = {
            'method': 'equals',
            'destination_attribute': 'dst_attr',
        }
        expected = "dst.dst_attr = json_obj_ref->>'bronwaarde'"
        self.assertEqual(expected, relater._relate_match(spec, 'src_ref'))

        spec['method'] = 'lies_in'
        self.assertEqual(relater._geo_resolve.return_value, relater._relate_match(spec, 'src_ref'))

    def test_dst_table_inner_join_on(self):
        relater = self._get_relater()
        relater.src_has_states = False
        relater.dst_has_states = False
        relater.relation_specs = [{'source': 'source1'}, {'source': 'source2'}]
        relater._relate_match = lambda spec, src_ref: 'relate_match(' + spec['source'] + ')'

        expected = [
            "((src_ref._application = 'source1' AND relate_match(source1)) OR\n"
            "    (src_ref._application = 'source2' AND relate_match(source2)))",
        ]
        self.assertEqual(expected, relater._dst_table_inner_join_on('src_ref'))

        relater.dst_has_states = True
        expected = [
            "((src_ref._application = 'source1' AND relate_match(source1)) OR\n"
            "    (src_ref._application = 'source2' AND relate_match(source2)))",
            '(dst.eind_geldigheid IS NULL OR dst.eind_geldigheid > NOW())',
        ]
        self.assertEqual(set(expected), set(relater._dst_table_inner_join_on('src_ref')))

        relater.src_has_states = True
        expected = [
            "((src_ref._application = 'source1' AND relate_match(source1)) OR\n    "
            "(src_ref._application = 'source2' AND relate_match(source2)))",
            '(dst.begin_geldigheid < src_ref.eind_geldigheid OR src_ref.eind_geldigheid IS NULL)',
            '(dst.eind_geldigheid >= src_ref.eind_geldigheid OR dst.eind_geldigheid IS NULL)',
        ]
        self.assertEqual(expected, relater._dst_table_inner_join_on('src_ref'))

    def test_table_outer_join_on(self):
        relater = self._get_relater()
        relater.dst_has_states = False

        expected = [
            'dst._id = src_dst.dst_id',
        ]

        self.assertEqual(expected, relater._dst_table_outer_join_on())

        relater.dst_has_states = True
        expected = [
            'dst._id = src_dst.dst_id',
            'dst.volgnummer = src_dst.dst_volgnummer',
        ]

        self.assertEqual(expected, relater._dst_table_outer_join_on())

    def test_src_dst_join_on_src(self):
        relater = self._get_relater()
        relater._json_obj_ref = MagicMock(return_value='json_obj_ref')
        relater.src_has_states = False

        expected = [
            "src_dst.src_id = src._id",
            "src_dst._source = src._source",
            "src_dst.bronwaarde = json_obj_ref->>'bronwaarde'"
        ]
        self.assertEqual(expected, relater._src_dst_join_on())

        relater.src_has_states = True
        expected = [
            "src_dst.src_id = src._id",
            "src_dst.src_volgnummer = src.volgnummer",
            "src_dst._source = src._source",
            "src_dst.bronwaarde = json_obj_ref->>'bronwaarde'"
        ]
        self.assertEqual(expected, relater._src_dst_join_on())

    def test_src_dst_join_on_dst(self):
        relater = self._get_relater()
        relater._json_obj_ref = MagicMock(return_value='json_obj_ref')
        relater.dst_has_states = False

        expected = [
            "src_dst.dst_id = dst._id",
        ]
        self.assertEqual(expected, relater._src_dst_join_on('dst'))

        relater.dst_has_states = True
        expected = [
            "src_dst.dst_id = dst._id",
            "src_dst.dst_volgnummer = dst.volgnummer",
        ]
        self.assertEqual(expected, relater._src_dst_join_on('dst'))

    def test_src_dst_join_on_invalid(self):
        relater = self._get_relater()

        with self.assertRaises(NotImplementedError):
            relater._src_dst_join_on('invalid')

    def test_src_dst_select_expressions(self):
        relater = self._get_relater()
        relater.src_has_states = False
        relater.dst_has_states = False
        relater._json_obj_ref = MagicMock(return_value='json_obj_ref')

        expected = [
            "src._id AS src_id",
            "dst._id AS dst_id",
            "json_obj_ref->>'bronwaarde' as bronwaarde",
            "src._source",
        ]
        self.assertEqual(expected, relater._src_dst_select_expressions())

        relater.src_has_states = True
        expected.append("src.volgnummer as src_volgnummer")
        self.assertEqual(expected, relater._src_dst_select_expressions())

        relater.dst_has_states = True
        expected.append("max(dst.volgnummer) as dst_volgnummer")
        self.assertEqual(expected, relater._src_dst_select_expressions())

    def test_src_dst_group_by(self):
        relater = self._get_relater()
        relater.src_has_states = False
        expected = [
            "src._id",
            "dst._id",
            "bronwaarde",
            "src._source",
        ]

        self.assertEqual(expected, relater._src_dst_group_by())

        relater.src_has_states = True
        expected.append("src.volgnummer")
        self.assertEqual(expected, relater._src_dst_group_by())

    def test_have_geo_specs(self):
        relater = self._get_relater()
        relater.relation_specs = [{'method': 'equals'}, {'method': 'equals'}]
        self.assertFalse(relater._have_geo_specs())

        relater.relation_specs.append({'method': 'lies_in'})
        self.assertTrue(relater._have_geo_specs())

    def test_valid_geo_src_check(self):
        relater = self._get_relater()
        relater.relation_specs = [
            {'source': 'sourceA', 'source_attribute': 'attrA'},
            {'source': 'sourceB', 'source_attribute': 'attrB'},
        ]
        expected = "(_application = 'sourceA' AND ST_IsValid(attrA)) OR\n    " \
                   "(_application = 'sourceB' AND ST_IsValid(attrB))"
        self.assertEqual(expected, relater._valid_geo_src_check())

    def test_join_array_elements(self):
        relater = self._get_relater()
        expected = "JOIN jsonb_array_elements(src.src_field_name) json_arr_elm(item) " \
                   "ON json_arr_elm->>'bronwaarde' IS NOT NULL"
        self.assertEqual(expected, relater._join_array_elements())

    def test_src_dst_select(self):
        relater = self._get_relater()

        self.assertEqual('SELECT * FROM src_entities WHERE _date_deleted IS NULL', relater._src_dst_select())
        self.assertEqual('SELECT * FROM src_entities WHERE _date_deleted IS NULL', relater._src_dst_select('src'))

        relater.src_has_states = False
        self.assertEqual('SELECT * FROM src_catalog_name_src_collection_name_table WHERE _date_deleted IS NULL '
                         'AND (_id) NOT IN (SELECT _id FROM src_entities)', relater._src_dst_select('dst'))
        relater.src_has_states = True
        self.assertEqual('SELECT * FROM src_catalog_name_src_collection_name_table WHERE _date_deleted IS NULL '
                         'AND (_id,volgnummer) NOT IN (SELECT _id,volgnummer FROM src_entities)',
                         relater._src_dst_select('dst'))

        with self.assertRaises(NotImplementedError):
            relater._src_dst_select('invalid')

    def _get_src_dst_join_mocked_relater(self):
        relater = self._get_relater()
        relater._valid_geo_src_check = lambda: 'CHECK_VALID_GEO_SRC'
        relater._src_dst_select_expressions = lambda: ['SRC_DST SELECT EXPRESSION1', 'SRC_DST SELECT EXPRESSION2']
        relater._join_array_elements = lambda: 'ARRAY_ELEMENTS'
        relater._dst_table_inner_join_on = lambda: ['DST_TABLE_INNER_JOIN_ON1', 'DST_TABLE_INNER_JOIN_ON2']
        relater._src_dst_group_by = lambda: ['SRC_DST_GROUP_BY1', 'SRC_DST_GROUP_BY2']
        relater._src_dst_join_on = lambda x: ['SRC_DST_JOIN_ON1(' + x + ')', 'SRC_DST_JOIN_ON2(' + x + ')']
        relater._src_dst_select = lambda x: 'SRC_DST_SELECT(' + x + ')'

        return relater

    def test_src_dst_join_no_geo_singleref(self):
        relater = self._get_src_dst_join_mocked_relater()
        relater._have_geo_specs = lambda: False
        relater.is_many = False

        expected = """
LEFT JOIN (
    SELECT
        SRC_DST SELECT EXPRESSION1,
    SRC_DST SELECT EXPRESSION2
    FROM (
        SRC_DST_SELECT(src)
    ) src
    
    
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst ON DST_TABLE_INNER_JOIN_ON1 AND
    DST_TABLE_INNER_JOIN_ON2
    AND dst._date_deleted IS NULL
    GROUP BY SRC_DST_GROUP_BY1,
    SRC_DST_GROUP_BY2
) src_dst ON SRC_DST_JOIN_ON1(src) AND
    SRC_DST_JOIN_ON2(src)
"""

        result = relater._src_dst_join()
        self.assertEqual(expected, result)

    def test_src_dst_join_no_geo_manyref(self):
        relater = self._get_src_dst_join_mocked_relater()
        relater._have_geo_specs = lambda: False
        relater.is_many = True

        expected = """
LEFT JOIN (
    SELECT
        SRC_DST SELECT EXPRESSION1,
    SRC_DST SELECT EXPRESSION2
    FROM (
        SRC_DST_SELECT(src)
    ) src
    
    ARRAY_ELEMENTS
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst ON DST_TABLE_INNER_JOIN_ON1 AND
    DST_TABLE_INNER_JOIN_ON2
    AND dst._date_deleted IS NULL
    GROUP BY SRC_DST_GROUP_BY1,
    SRC_DST_GROUP_BY2
) src_dst ON SRC_DST_JOIN_ON1(src) AND
    SRC_DST_JOIN_ON2(src)
"""

        result = relater._src_dst_join()
        self.assertEqual(expected, result)

    def test_src_dst_join_geo_singleref(self):
        relater = self._get_src_dst_join_mocked_relater()
        relater._have_geo_specs = lambda: True
        relater.is_many = False

        expected = """
LEFT JOIN (
    SELECT
        SRC_DST SELECT EXPRESSION1,
    SRC_DST SELECT EXPRESSION2
    FROM (
        SRC_DST_SELECT(src)
    ) src
    JOIN (SELECT * FROM src_catalog_name_src_collection_name_table WHERE (CHECK_VALID_GEO_SRC)) valid_src ON src._gobid = valid_src._gobid
    
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst ON DST_TABLE_INNER_JOIN_ON1 AND
    DST_TABLE_INNER_JOIN_ON2
    AND dst._date_deleted IS NULL
    GROUP BY SRC_DST_GROUP_BY1,
    SRC_DST_GROUP_BY2
) src_dst ON SRC_DST_JOIN_ON1(src) AND
    SRC_DST_JOIN_ON2(src)
"""

        result = relater._src_dst_join()
        self.assertEqual(expected, result)

    def test_src_dst_join_geo_manyref(self):
        relater = self._get_src_dst_join_mocked_relater()
        relater._have_geo_specs = lambda: True
        relater.is_many = True

        expected = """
LEFT JOIN (
    SELECT
        SRC_DST SELECT EXPRESSION1,
    SRC_DST SELECT EXPRESSION2
    FROM (
        SRC_DST_SELECT(src)
    ) src
    JOIN (SELECT * FROM src_catalog_name_src_collection_name_table WHERE (CHECK_VALID_GEO_SRC)) valid_src ON src._gobid = valid_src._gobid
    ARRAY_ELEMENTS
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst ON DST_TABLE_INNER_JOIN_ON1 AND
    DST_TABLE_INNER_JOIN_ON2
    AND dst._date_deleted IS NULL
    GROUP BY SRC_DST_GROUP_BY1,
    SRC_DST_GROUP_BY2
) src_dst ON SRC_DST_JOIN_ON1(src) AND
    SRC_DST_JOIN_ON2(src)
"""

        result = relater._src_dst_join()
        self.assertEqual(expected, result)

    def test_start_validitity_per_seqnr_src(self):
        relater = self._get_relater()
        expected = """
all_src_intervals(
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
    FROM src_catalog_name_src_collection_name_table s
    LEFT JOIN src_catalog_name_src_collection_name_table t
    ON s._id = t._id
        AND t.volgnummer::int < s.volgnummer::int
        AND t.eind_geldigheid = s.begin_geldigheid
    WHERE t._id IS NULL
    UNION
    SELECT
        intv._id,
        intv.start_volgnummer,
        src.volgnummer,
        intv.begin_geldigheid,
        src.eind_geldigheid
    FROM all_src_intervals intv
    LEFT JOIN src_catalog_name_src_collection_name_table src
    ON intv.eind_geldigheid = src.begin_geldigheid
        AND src._id = intv._id
        AND src.volgnummer::int > intv.volgnummer::int
    WHERE src.begin_geldigheid IS NOT NULL
), src_volgnummer_begin_geldigheid AS (
    SELECT
        _id,
        volgnummer,
        MIN(begin_geldigheid) begin_geldigheid
    FROM all_src_intervals
    GROUP BY _id, volgnummer
)"""

        self.assertEqual(expected, relater._start_validity_per_seqnr('src'))

    def test_start_validitity_per_seqnr_dst(self):
        relater = self._get_relater()
        expected = """
all_dst_intervals(
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
        dst.volgnummer,
        intv.begin_geldigheid,
        dst.eind_geldigheid
    FROM all_dst_intervals intv
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst
    ON intv.eind_geldigheid = dst.begin_geldigheid
        AND dst._id = intv._id
        AND dst.volgnummer::int > intv.volgnummer::int
    WHERE dst.begin_geldigheid IS NOT NULL
), dst_volgnummer_begin_geldigheid AS (
    SELECT
        _id,
        volgnummer,
        MIN(begin_geldigheid) begin_geldigheid
    FROM all_dst_intervals
    GROUP BY _id, volgnummer
)"""

        self.assertEqual(expected, relater._start_validity_per_seqnr('dst'))

    def test_start_validities(self):
        relater = self._get_relater()
        relater._start_validity_per_seqnr = lambda x: 'VALIDITIES_FOR_' + x.upper()

        relater.src_has_states = False
        relater.dst_has_states = False

        self.assertEqual([], relater._start_validities())

        relater.src_has_states = True
        self.assertEqual(["VALIDITIES_FOR_SRC"], relater._start_validities())

        relater.dst_has_states = True
        self.assertEqual(["VALIDITIES_FOR_SRC", "VALIDITIES_FOR_DST"], relater._start_validities())

        relater.src_has_states = False
        self.assertEqual(["VALIDITIES_FOR_DST"], relater._start_validities())

    def test_with_src_entities(self):
        relater = self._get_relater()
        expected = f"""
src_entities AS (
    SELECT * FROM src_catalog_name_src_collection_name_table WHERE _last_event > (
        SELECT COALESCE(MAX(_last_src_event), 0) FROM rel_src_catalog_name_src_collection_name_src_field_name
    )
)
"""
        self.assertEqual(expected, relater._with_src_entities())

    def test_with_dst_entities(self):
        relater = self._get_relater()
        expected = f"""
dst_entities AS (
    SELECT * FROM dst_catalog_name_dst_collection_name_table WHERE _last_event > (
        SELECT COALESCE(MAX(_last_dst_event), 0) FROM rel_src_catalog_name_src_collection_name_src_field_name
    )
)
"""
        self.assertEqual(expected, relater._with_dst_entities())

    def test_with_queries(self):
        relater = self._get_relater()
        relater._with_src_entities = lambda: 'SRC ENTITIES'
        relater._with_dst_entities = lambda: 'DST ENTITIES'
        relater._start_validities = lambda: []
        self.assertEqual('WITH SRC ENTITIES,DST ENTITIES', relater._with_queries())

        relater._start_validities = lambda: ['START_VALIDITIES1', 'START_VALIDITIES2']
        self.assertEqual('WITH RECURSIVE START_VALIDITIES1,START_VALIDITIES2,SRC ENTITIES,DST ENTITIES',
                         relater._with_queries())

    def test_join_src_geldigheid(self):
        relater = self._get_relater()
        relater.src_has_states = False
        self.assertEqual("", relater._join_src_geldigheid())
        relater.src_has_states = True
        self.assertEqual("LEFT JOIN src_volgnummer_begin_geldigheid src_bg "
                         "ON src_bg._id = src._id AND src_bg.volgnummer = src.volgnummer",
                         relater._join_src_geldigheid())

    def test_join_dst_geldigheid(self):
        relater = self._get_relater()
        relater.dst_has_states = False
        self.assertEqual("", relater._join_dst_geldigheid())
        relater.dst_has_states = True
        self.assertEqual("LEFT JOIN dst_volgnummer_begin_geldigheid dst_bg "
                         "ON dst_bg._id = dst._id AND dst_bg.volgnummer = dst.volgnummer",
                         relater._join_dst_geldigheid())

    def test_join_rel(self):
        relater = self._get_relater()
        relater.src_has_states = False

        self.assertEqual(f"""
FULL JOIN (
    SELECT * FROM rel_src_catalog_name_src_collection_name_src_field_name
    WHERE src_id IN (SELECT _id FROM src_entities)
    AND _date_deleted IS NULL
) rel ON rel.src_id = src._id AND src.src_field_name->>'bronwaarde' = rel.bronwaarde
""", relater._join_rel())

        relater.src_has_states = True

        self.assertEqual(f"""
FULL JOIN (
    SELECT * FROM rel_src_catalog_name_src_collection_name_src_field_name
    WHERE (src_id, src_volgnummer) IN (SELECT _id, volgnummer FROM src_entities)
    AND _date_deleted IS NULL
) rel ON rel.src_id = src._id AND rel.src_volgnummer = src.volgnummer
    AND src.src_field_name->>'bronwaarde' = rel.bronwaarde
""", relater._join_rel())

    def _get_get_query_mocked_relater(self):
        relater = self._get_relater()
        relater._dst_table_outer_join_on = lambda: ['DST_TABLE_OUTER_JOIN_ON1', 'DST_TABLE_OUTER_JOIN_ON2']
        relater._select_expressions_src = lambda: ['SELECT_EXPRESSION1SRC', 'SELECT_EXPRESSION2SRC']
        relater._select_expressions_dst = lambda: ['SELECT_EXPRESSION1DST', 'SELECT_EXPRESSION2DST']
        relater._join_array_elements = lambda: 'ARRAY_ELEMENTS'
        relater._src_dst_join = lambda x: 'SRC_DST_JOIN(' + x + ')'
        relater._start_validities = lambda: 'SEQNR_BEGIN_GELDIGHEID'
        relater._join_src_geldigheid = lambda: 'JOIN_SRC_GELDIGHEID'
        relater._join_dst_geldigheid = lambda: 'JOIN_DST_GELDIGHEID'
        relater._with_queries = lambda: 'WITH QUERIES'
        relater._get_where = lambda: 'WHERE CLAUSE'
        relater._join_rel = lambda: 'JOIN REL'

        return relater

    def test_get_query_singleref(self):
        relater = self._get_get_query_mocked_relater()
        relater.is_many = False

        expected = """
WITH QUERIES
SELECT
    SELECT_EXPRESSION1SRC,
    SELECT_EXPRESSION2SRC
FROM src_entities src


SRC_DST_JOIN(src)
LEFT JOIN dst_catalog_name_dst_collection_name_table dst
    ON DST_TABLE_OUTER_JOIN_ON1 AND
    DST_TABLE_OUTER_JOIN_ON2
JOIN REL
JOIN_SRC_GELDIGHEID
JOIN_DST_GELDIGHEID


UNION

SELECT
    SELECT_EXPRESSION1DST,
    SELECT_EXPRESSION2DST
FROM dst_entities dst
SRC_DST_JOIN(dst)
INNER JOIN src_catalog_name_src_collection_name_table src
    ON src_dst.src_id = src._id AND src_dst.src_volgnummer = src.volgnummer
    AND src_dst._source = src._source
INNER JOIN rel_src_catalog_name_src_collection_name_src_field_name rel
    ON rel.src_id=src._id AND rel.src_volgnummer = src.volgnummer
    AND rel.src_source = src._source AND rel.bronwaarde = src_dst.bronwaarde
JOIN_SRC_GELDIGHEID
JOIN_DST_GELDIGHEID
"""

        result = relater._get_query()
        self.assertEqual(result, expected)

    def test_get_query_manyref(self):
        relater = self._get_get_query_mocked_relater()
        relater.is_many = True

        expected = """
WITH QUERIES
SELECT
    SELECT_EXPRESSION1SRC,
    SELECT_EXPRESSION2SRC
FROM src_entities src

ARRAY_ELEMENTS
SRC_DST_JOIN(src)
LEFT JOIN dst_catalog_name_dst_collection_name_table dst
    ON DST_TABLE_OUTER_JOIN_ON1 AND
    DST_TABLE_OUTER_JOIN_ON2
JOIN REL
JOIN_SRC_GELDIGHEID
JOIN_DST_GELDIGHEID


UNION

SELECT
    SELECT_EXPRESSION1DST,
    SELECT_EXPRESSION2DST
FROM dst_entities dst
SRC_DST_JOIN(dst)
INNER JOIN src_catalog_name_src_collection_name_table src
    ON src_dst.src_id = src._id AND src_dst.src_volgnummer = src.volgnummer
    AND src_dst._source = src._source
INNER JOIN rel_src_catalog_name_src_collection_name_src_field_name rel
    ON rel.src_id=src._id AND rel.src_volgnummer = src.volgnummer
    AND rel.src_source = src._source AND rel.bronwaarde = src_dst.bronwaarde
JOIN_SRC_GELDIGHEID
JOIN_DST_GELDIGHEID
"""
        result = relater._get_query()
        self.assertEqual(result, expected)

    def test_get_modifications(self):
        relater = self._get_relater()

        row = {
            'rel_a': 'a field',
            'rel_b': 'b field',
            'rel_c': 'c field',
            'a': 'a field',
            'b': 'b field',
            'c': 'changed field',
        }

        self.assertEqual([], relater._get_modifications(row, ['a', 'b']))

        self.assertEqual([
            {
                'old_value': 'c field',
                'new_value': 'changed field',
                'key': 'c'
            }
        ], relater._get_modifications(row, ['a', 'b', 'c']))

    @patch('gobupload.relate.table.update_table.ADD')
    @patch('gobupload.relate.table.update_table.MODIFY')
    @patch('gobupload.relate.table.update_table.DELETE')
    @patch('gobupload.relate.table.update_table.CONFIRM')
    def test_create_event(self, mock_confirm, mock_delete, mock_modify, mock_add):
        relater = self._get_relater()
        relater.dst_has_states = False

        # ADD event
        row = {
            'rel_id': None,
            '_source_id': 'SOURCE ID',
            'a': 'val',
            'b': 'val',
        }
        event = relater._create_event(row)
        self.assertEqual(mock_add.create_event.return_value, event)
        mock_add.create_event.assert_called_with('SOURCE ID', 'SOURCE ID', {
            '_source_id': 'SOURCE ID',
            'a': 'val',
            'b': 'val',
        })

        # DELETE EVENT (src not present)
        row = {
            'rel_id': 'rel id',
            '_source_id': 'SOURCE ID',
            'a': 'val',
            'b': 'val',
            'src_id': None,
            '_last_event': 'last',
            'src_deleted': None,
        }
        event = relater._create_event(row)
        self.assertEqual(mock_delete.create_event.return_value, event)
        mock_delete.create_event.assert_called_with('rel id', 'rel id', {'_last_event': 'last'})

        # DELETE EVENT (src marked as deleted)
        row = {
            'rel_id': 'rel id',
            '_source_id': 'SOURCE ID',
            'a': 'val',
            'b': 'val',
            'src_id': 'src id',
            '_last_event': 'last',
            'src_deleted': True,
        }
        event = relater._create_event(row)
        self.assertEqual(mock_delete.create_event.return_value, event)
        mock_delete.create_event.assert_called_with('rel id', 'rel id', {'_last_event': 'last'})

        # MODIFY EVENT (hash differs, and modifications detected)
        relater._get_hash = lambda x: 'THE HASH'
        relater._get_modifications = lambda a, b: ['a']
        row = {
            'rel_id': 'rel id',
            'src_deleted': None,
            'src_id': 'src id',
            'rel__hash': 'DIFFERENT HASH',
            '_last_event': 'last',
        }
        event = relater._create_event(row)
        self.assertEqual(mock_modify.create_event.return_value, event)
        mock_modify.create_event.assert_called_with('rel id', 'rel id', {
            'modifications': ['a'],
            '_last_event': 'last',
            '_hash': 'THE HASH'
        })

        # CONFIRM EVENT
        row = {
            'rel_id': 'rel id',
            'src_deleted': None,
            'src_id': 'src id',
            'rel__hash': 'THE HASH',
            '_last_event': 'last',
        }
        event = relater._create_event(row)
        self.assertEqual(mock_confirm.create_event.return_value, event)
        mock_confirm.create_event.assert_called_with('rel id', 'rel id', {'_last_event': 'last'})

    def test_format_relation(self):
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

        updater = self._get_relater()
        self.assertEqual(expected_result, [updater._format_relation(relation) for relation in rows])

    @patch("gobupload.relate.table.update_table._execute")
    @patch("gobupload.relate.table.update_table.EventCollector")
    @patch("gobupload.relate.table.update_table.ContentsWriter")
    @patch("gobupload.relate.table.update_table.ProgressTicker", MagicMock())
    def test_update(self, mock_contents_writer, mock_event_collector, mock_execute):
        relater = self._get_relater()
        relater._create_event = MagicMock(side_effect=lambda x: x)
        relater._get_query = MagicMock()
        relater._format_relation = MagicMock(side_effect=lambda x: x)
        mock_execute.return_value = [{'a': 1}, {'b': 2}, {'c': 3}]

        result = relater.update()
        mock_execute.assert_called_with(relater._get_query.return_value, stream=True)
        mock_event_collector.assert_called_with(
            mock_contents_writer.return_value.__enter__.return_value,
            mock_contents_writer.return_value.__enter__.return_value,
        )
        mock_event_collector.return_value.__enter__.return_value.collect.assert_has_calls([
            call({'a': 1}),
            call({'b': 2}),
            call({'c': 3}),
        ])

        self.assertEqual((mock_contents_writer.return_value.__enter__.return_value.filename,
                          mock_contents_writer.return_value.__enter__.return_value.filename), result)
