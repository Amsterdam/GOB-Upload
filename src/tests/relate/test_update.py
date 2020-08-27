from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobcore.model.metadata import FIELD
from gobcore.exceptions import GOBException
from datetime import date, datetime
from gobupload.relate.update import Relater, RelateException, StartValiditiesTable


class MockModel:
    _has_states = {
        'src_collection_name': True,
        'dst_collection_name': False,
    }

    def get_collection(self, cat, coll):
        if coll == 'src_collection_name':
            return {
                'all_fields': {
                    'src_field_name': {
                        'type': 'GOB.Reference',
                        'ref': 'dst_catalog_name:dst_collection_name',
                    }
                },
                'abbreviation': 'srcabbr'
            }
        else:
            return {
                'abbreviation': 'dstabbr'
            }

    def get_table_name(self, catalog_name, collection_name):
        return f"{catalog_name}_{collection_name}_table"

    def has_states(self, catalog_name, collection_name):
        return self._has_states[collection_name]


class MockSources:
    def get_field_relations(self, *args):
        return [{
            'source': 'applicationA'
        }, {
            'source': 'applicationB',
            'multiple_allowed': True
        }]


class TestStartValiditiesTable(TestCase):

    def test_query(self):
        table = StartValiditiesTable('from_table_name', 'to_table')
        expected = """
WITH RECURSIVE
all_intervals(
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
    FROM from_table_name s
    LEFT JOIN from_table_name t
    ON s._id = t._id
        AND t.volgnummer < s.volgnummer
        AND t.eind_geldigheid = s.begin_geldigheid
    WHERE t._id IS NULL
    UNION
    SELECT
        intv._id,
        intv.start_volgnummer,
        t.volgnummer,
        intv.begin_geldigheid,
        t.eind_geldigheid
    FROM all_intervals intv
    LEFT JOIN from_table_name t
    ON intv.eind_geldigheid = t.begin_geldigheid
        AND t._id = intv._id
        AND t.volgnummer > intv.volgnummer
    WHERE t.begin_geldigheid IS NOT NULL
)
SELECT
    _id,
    volgnummer,
    MIN(begin_geldigheid) begin_geldigheid
FROM all_intervals
GROUP BY _id, volgnummer
"""

        self.assertTrue(expected in table._query())

    @patch("gobupload.relate.update._execute")
    def test_create(self, mock_execute):
        table = StartValiditiesTable('from_table_name', 'to_table')
        table.drop = MagicMock()
        table._query = MagicMock(return_value='the query')

        table.create()

        table.drop.assert_called_once()
        mock_execute.assert_has_calls([
            call("CREATE TABLE IF NOT EXISTS to_table AS (the query)"),
            call("CREATE INDEX ON to_table(_id, volgnummer)")
        ])

    @patch("gobupload.relate.update._execute")
    def test_drop(self, mock_execute):
        table = StartValiditiesTable('from_table_name', 'to_table')
        table.drop()
        mock_execute.assert_called_with("DROP TABLE IF EXISTS to_table")

    @patch("gobupload.relate.update.GOBModel", MockModel)
    def test_from_catalog_collection(self):
        res = StartValiditiesTable.from_catalog_collection('catalog_name', 'collection_name', 'table_name')

        self.assertEqual('catalog_name_collection_name_table', res.from_table)
        self.assertEqual('table_name', res.name)

@patch("gobupload.relate.update.logger", MagicMock())
@patch("gobupload.relate.update.Relater.model", MockModel())
@patch("gobupload.relate.update.Relater.sources", MockSources())
@patch("gobupload.relate.update._execute")
@patch("gobupload.relate.update.random_string", lambda x: x * 'a')
@patch("gobupload.relate.update.get_relation_name", lambda m, cat, col, field: f"{cat}_{col}_{field}")
class TestRelaterInit(TestCase):

    def _get_relater(self):
        return Relater('src_catalog_name', 'src_collection_name', 'src_field_name')

    @patch("gobupload.relate.update.datetime")
    def test_init(self, mock_datetime, mock_execute):
        mock_execute.return_value = [('applicationA',)]
        mock_datetime.now.return_value.strftime.return_value = '20200101'

        e = self._get_relater()

        self.assertEqual('src_catalog_name', e.src_catalog_name)
        self.assertEqual('src_collection_name', e.src_collection_name)
        self.assertEqual('src_field_name', e.src_field_name)
        self.assertEqual(MockModel().get_collection('', 'src_collection_name'), e.src_collection)
        self.assertEqual(MockModel().get_collection('', 'src_collection_name')['all_fields']['src_field_name'], e.src_field)
        self.assertEqual('dst_catalog_name_dst_collection_name_table', e.dst_table_name)
        self.assertEqual(True, e.src_has_states)
        self.assertEqual(False, e.dst_has_states)
        self.assertEqual([{
            # applicationB should be filtered out
            'source': 'applicationA',
            'multiple_allowed': False,
        }], e.relation_specs)
        self.assertEqual(False, e.is_many)
        self.assertEqual('rel_src_catalog_name_src_collection_name_src_field_name', e.relation_table)
        mock_execute.assert_called_with("SELECT DISTINCT _application FROM src_catalog_name_src_collection_name_table")

        self.assertEqual('tmp_src_catalog_name_srcabbr_intv_20200101_aaaaaa', e.src_intv_tmp_table.name)
        self.assertEqual('tmp_dst_catalog_name_dstabbr_intv_20200101_aaaaaa', e.dst_intv_tmp_table.name)

        with patch('gobupload.relate.update.Relater.sources.get_field_relations',
                   lambda cat, col, field: []):
            with self.assertRaises(RelateException):
                self._get_relater()

        with patch('gobupload.relate.update.Relater.model.get_collection',
                   lambda cat, col: {'all_fields': {
                       'src_field_name': {
                           'type': 'GOB.ManyReference',
                           'ref': 'dst_catalog_name:dst_collection_name',
                       }}, 'abbreviation': 'abbr'}):
            e = self._get_relater()
            self.assertEqual(True, e.is_many)


@patch("gobupload.relate.update.logger", MagicMock())
@patch("gobupload.relate.update.Relater.model", MockModel())
@patch("gobupload.relate.update.Relater.sources", MockSources())
@patch("gobupload.relate.update.Relater._get_applications_in_src", lambda *args: ['applicationA', 'applicationB'])
@patch("gobupload.relate.update.get_relation_name", lambda m, cat, col, field: f"{cat}_{col}_{field}")
class TestRelater(TestCase):

    def _get_relater(self):
        return Relater('src_catalog_name', 'src_collection_name', 'src_field_name')

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

    def test_select_aliases(self):
        relater = self._get_relater()
        relater.select_aliases = ['a']
        relater.select_relation_aliases = ['b']
        relater.src_has_states = False
        relater.dst_has_states = False

        self.assertEqual(['a', 'b'], relater._select_aliases())

        relater.src_has_states = True
        relater.dst_has_states = True

        self.assertEqual([
            'a',
            'b',
            'src_volgnummer',
            'dst_volgnummer',
        ], relater._select_aliases())

        relater.exclude_relation_table = True

        self.assertEqual([
            'a',
            'src_volgnummer',
            'dst_volgnummer',
        ], relater._select_aliases())

    def test_build_select_expressions(self):
        relater = self._get_relater()
        relater.src_has_states = False
        relater.dst_has_states = False
        relater._select_aliases = lambda: ['a', 'b']

        mapping = {'a': 'some a', 'b': 'some b'}
        self.assertEqual([
            'some a AS a',
            'some b AS b',
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
        relater._get_id = lambda _: 'ID'
        relater._get_derivation = lambda: 'DERIVATION'
        relater._source_value_ref = lambda: 'SOURCE VALUE'
        relater._build_select_expressions = MagicMock()
        relater._validity_select_expressions = lambda: ('START_VALIDITY', 'END_VALIDITY')
        result = relater._select_expressions_dst()
        self.assertEqual(relater._build_select_expressions.return_value, result)

    def test_select_expressions_rel_delete(self):
        relater = self._get_relater()
        res = relater._select_expressions_rel_delete()
        expected = ['NULL AS _version',
                    'NULL AS _application',
                    'NULL AS _source_id',
                    'NULL AS _source',
                    'NULL AS _expiration_date',
                    'NULL AS id',
                    'NULL AS derivation',
                    'NULL AS src_source',
                    'NULL AS src_id',
                    'NULL AS src_last_event',
                    'NULL AS dst_source',
                    'NULL AS dst_id',
                    'NULL AS bronwaarde',
                    'NULL AS _last_src_event',
                    'NULL AS _last_dst_event',
                    'NULL AS begin_geldigheid',
                    'NULL AS eind_geldigheid',
                    'NULL AS src_deleted',
                    'NULL AS row_number',
                    'rel._last_event AS _last_event',
                    'NULL AS rel_deleted',
                    'rel.id AS rel_id',
                    'rel.dst_id AS rel_dst_id',
                    'rel.dst_volgnummer AS rel_dst_volgnummer',
                    'rel._expiration_date AS rel__expiration_date',
                    'rel.begin_geldigheid AS rel_begin_geldigheid',
                    'rel.eind_geldigheid AS rel_eind_geldigheid',
                    'rel._hash AS rel__hash',
                    'NULL AS src_volgnummer']

        self.assertEqual(expected, res)

    def test_get_id(self):
        relater = self._get_relater()
        relater._source_value_ref = MagicMock(return_value='BRONWAARDE')

        no_src_states = f"""CASE WHEN src._application = 'applicationA' THEN src._id || '.' || src._source || '.' || (BRONWAARDE)
WHEN src._application = 'applicationB' THEN src._id || '.' || src._source || '.' || (BRONWAARDE) || '.' || dst._id
END"""
        with_src_states = f"""CASE WHEN src._application = 'applicationA' THEN src._id || '.' || src.volgnummer || '.' || src._source || '.' || (BRONWAARDE)
WHEN src._application = 'applicationB' THEN src._id || '.' || src.volgnummer || '.' || src._source || '.' || (BRONWAARDE) || '.' || dst._id
END"""
        relater.src_has_states = False
        self.assertEqual(no_src_states, relater._get_id())
        relater.src_has_states = True
        self.assertEqual(with_src_states, relater._get_id())

        with_src_val_ref = f"""CASE WHEN src._application = 'applicationA' THEN src._id || '.' || src.volgnummer || '.' || src._source || '.' || (the src value ref)
WHEN src._application = 'applicationB' THEN src._id || '.' || src.volgnummer || '.' || src._source || '.' || (the src value ref) || '.' || dst._id
END"""
        self.assertEqual(with_src_val_ref, relater._get_id('the src value ref'))

        # No specs, NULL
        relater.relation_specs = []
        self.assertEqual('NULL', relater._get_id())

        # No CASE statement, just taking the first because only one relation spec
        relater.relation_specs = [{
            'multiple_allowed': False,
        }]
        self.assertEqual("src._id || '.' || src.volgnummer || '.' || src._source || '.' || (BRONWAARDE)", relater._get_id())

    def test_get_id_for_dst(self):
        relater = self._get_relater()
        relater._get_id = MagicMock()

        self.assertEqual(relater._get_id.return_value, relater._get_id_for_dst())
        relater._get_id.assert_called_with('src.bronwaarde')

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

        # No specs, NULL
        relater.relation_specs = []
        self.assertEqual('NULL', relater._get_derivation())

        # No CASE statement, just taking the first because only one relation spec
        relater.relation_specs = [{
            'destination_attribute': 'attrA'
        }]
        self.assertEqual("'attrA'", relater._get_derivation())

    def test_source_value_ref(self):
        relater = self._get_relater()
        relater.is_many = False

        self.assertEqual("src.src_field_name->>'bronwaarde'", relater._source_value_ref())

        relater.is_many = True
        self.assertEqual("json_arr_elm.item->>'bronwaarde'", relater._source_value_ref())

    def test_geo_resolve(self):
        expected = "ST_IsValid(dst.dst_attribute) AND " \
                   "ST_Contains(dst.dst_attribute::geometry, ST_PointOnSurface(src.src_attribute::geometry))"

        relater = self._get_relater()

        spec = {
            'destination_attribute': 'dst_attribute',
            'source_attribute': 'src_attribute',
            'method': 'lies_in',
        }
        self.assertEqual(expected, relater._geo_resolve(spec))

    def test_json_obj_ref(self):
        relater = self._get_relater()
        relater.is_many = True
        expected = 'json_arr_elm.item'
        self.assertEqual(expected, relater._json_obj_ref())

        relater.is_many = False
        expected = 'src.src_field_name'
        self.assertEqual(expected, relater._json_obj_ref())

    def test_relate_match(self):
        relater = self._get_relater()
        relater._geo_resolve = MagicMock()
        relater._json_obj_ref = MagicMock(return_value='json_obj_ref')

        spec = {
            'method': 'equals',
            'destination_attribute': 'dst_attr',
        }
        expected = "dst.dst_attr = json_obj_ref->>'bronwaarde'"
        self.assertEqual(expected, relater._relate_match(spec))

        # Override source_value_ref
        expected = "dst.dst_attr = source_value_ref"
        self.assertEqual(expected, relater._relate_match(spec, 'source_value_ref'))

        # Geo match
        spec['method'] = 'lies_in'
        self.assertEqual(relater._geo_resolve.return_value, relater._relate_match(spec))

        # Test with source_attribute set
        spec['source_attribute'] = 'src_attr'
        spec['method'] = 'equals'
        expected = "dst.dst_attr = src.src_attr"
        self.assertEqual(expected, relater._relate_match(spec))

    def test_src_dst_match(self):
        relater = self._get_relater()
        relater.src_has_states = False
        relater.dst_has_states = False
        relater.relation_specs = [{'source': 'source1'}, {'source': 'source2'}]
        relater._relate_match = lambda spec, src_val_ref: 'relate_match(' + spec['source'] + ',' + src_val_ref + ')'

        expected = [
            "((src._application = 'source1' AND relate_match(source1,src_val_ref)) OR\n"
            "    (src._application = 'source2' AND relate_match(source2,src_val_ref)))",
        ]
        self.assertEqual(expected, relater._src_dst_match('src_val_ref'))

        relater.dst_has_states = True
        expected = [
            "((src._application = 'source1' AND relate_match(source1,src_val_ref)) OR\n"
            "    (src._application = 'source2' AND relate_match(source2,src_val_ref)))",
            '(dst.eind_geldigheid IS NULL OR dst.eind_geldigheid > NOW())',
        ]
        self.assertEqual(set(expected), set(relater._src_dst_match('src_val_ref')))

        relater.src_has_states = True
        expected = [
            "((src._application = 'source1' AND relate_match(source1,src_val_ref)) OR\n    "
            "(src._application = 'source2' AND relate_match(source2,src_val_ref)))",

            '(((dst.begin_geldigheid < src.eind_geldigheid OR src.eind_geldigheid IS NULL) AND\n    '
            '(dst.eind_geldigheid >= src.eind_geldigheid OR dst.eind_geldigheid IS NULL) AND\n    '
            '(src.eind_geldigheid IS NULL OR src.begin_geldigheid <> src.eind_geldigheid)) OR\n    '
            '((dst.begin_geldigheid <= src.eind_geldigheid) AND\n    '
            '(dst.eind_geldigheid >= src.eind_geldigheid OR dst.eind_geldigheid IS NULL) AND\n    '
            '(src.begin_geldigheid = src.eind_geldigheid)))'
        ]
        self.assertEqual(expected, relater._src_dst_match('src_val_ref'))

        # Simplified version, only one application, should leave out the OR
        relater.relation_specs = [{'source': 'source1'}]
        relater.dst_has_states = False
        relater.src_has_states = False
        expected = [
            'relate_match(source1,src_val_ref)'
        ]
        self.assertEqual(expected, relater._src_dst_match('src_val_ref'))


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

    def test_src_dst_join_on(self):
        relater = self._get_relater()
        relater._json_obj_ref = MagicMock(return_value='json_obj_ref')
        relater.src_has_states = False

        expected = [
            "src_dst.src_id = src._id",
            "src_dst._source = src._source",
            "src_dst.bronwaarde = json_obj_ref->>'bronwaarde'",
            "((src._application = 'applicationA' AND src_dst.row_number = 1) OR (src._application = 'applicationB'))"
        ]
        self.assertEqual(expected, relater._src_dst_join_on())

        relater.src_has_states = True
        expected = [
            "src_dst.src_id = src._id",
            "src_dst.src_volgnummer = src.volgnummer",
            "src_dst._source = src._source",
            "src_dst.bronwaarde = json_obj_ref->>'bronwaarde'",
            "((src._application = 'applicationA' AND src_dst.row_number = 1) OR (src._application = 'applicationB'))"
        ]
        self.assertEqual(expected, relater._src_dst_join_on())

        relater.exclude_relation_table = True
        expected = [
            "src_dst.src_id = src._id",
            "src_dst.src_volgnummer = src.volgnummer",
            "src_dst._source = src._source",
            "src_dst.bronwaarde = json_obj_ref->>'bronwaarde'"
        ]
        self.assertEqual(expected, relater._src_dst_join_on())

    def test_src_dst_select_expressions(self):
        relater = self._get_relater()
        relater.src_has_states = False
        relater.dst_has_states = False
        relater._row_number_partition = lambda: 'ROW_NUMBER_PARTITION'
        relater._json_obj_ref = MagicMock(return_value='json_obj_ref')

        expected = [
            "src._id AS src_id",
            "dst._id AS dst_id",
            "json_obj_ref->>'bronwaarde' AS bronwaarde",
            "src._source",
            "ROW_NUMBER_PARTITION AS row_number"
        ]
        self.assertEqual(expected, relater._src_dst_select_expressions())

        relater.src_has_states = True
        expected.append("src.volgnummer AS src_volgnummer")
        self.assertEqual(expected, relater._src_dst_select_expressions())

        relater.dst_has_states = True
        expected.append("max(dst.volgnummer) AS dst_volgnummer")
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
            {'source': 'applicationA', 'source_attribute': 'attrA'},
            {'source': 'applicationB', 'source_attribute': 'attrB'},
        ]
        expected = "((src._application = 'applicationA' AND ST_IsValid(attrA)) OR " \
                   "(src._application = 'applicationB' AND ST_IsValid(attrB)))"
        self.assertEqual(expected, relater._valid_geo_src_check())

    def test_join_array_elements(self):
        relater = self._get_relater()
        expected = "JOIN jsonb_array_elements(src.src_field_name) json_arr_elm(item) " \
                   "ON json_arr_elm->>'bronwaarde' IS NOT NULL"
        self.assertEqual(expected, relater._join_array_elements())

    def test_src_dst_select(self):
        relater = self._get_relater()

        self.assertEqual('SELECT * FROM src_entities WHERE _date_deleted IS NULL', relater._src_dst_select())

    def _get_src_dst_join_mocked_relater(self):
        relater = self._get_relater()
        relater._valid_geo_src_check = lambda: 'CHECK_VALID_GEO_SRC'
        relater._src_dst_select_expressions = lambda: ['SRC_DST SELECT EXPRESSION1', 'SRC_DST SELECT EXPRESSION2']
        relater._join_array_elements = lambda: 'ARRAY_ELEMENTS'
        relater._src_dst_match = lambda: ['DST_TABLE_INNER_JOIN_ON1', 'DST_TABLE_INNER_JOIN_ON2']
        relater._src_dst_group_by = lambda: ['SRC_DST_GROUP_BY1', 'SRC_DST_GROUP_BY2']
        relater._src_dst_join_on = lambda: ['SRC_DST_JOIN_ON1', 'SRC_DST_JOIN_ON2']
        relater._src_dst_select = lambda: 'SRC_DST_SELECT'

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
        SRC_DST_SELECT
    ) src
    
    
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst ON DST_TABLE_INNER_JOIN_ON1 AND
    DST_TABLE_INNER_JOIN_ON2 AND
    dst._date_deleted IS NULL
    GROUP BY SRC_DST_GROUP_BY1,
    SRC_DST_GROUP_BY2
) src_dst ON SRC_DST_JOIN_ON1 AND
    SRC_DST_JOIN_ON2
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
        SRC_DST_SELECT
    ) src
    
    ARRAY_ELEMENTS
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst ON DST_TABLE_INNER_JOIN_ON1 AND
    DST_TABLE_INNER_JOIN_ON2 AND
    dst._date_deleted IS NULL
    GROUP BY SRC_DST_GROUP_BY1,
    SRC_DST_GROUP_BY2
) src_dst ON SRC_DST_JOIN_ON1 AND
    SRC_DST_JOIN_ON2
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
        SRC_DST_SELECT
    ) src
    JOIN (SELECT * FROM src_catalog_name_src_collection_name_table WHERE (CHECK_VALID_GEO_SRC)) valid_src ON src._gobid = valid_src._gobid
    
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst ON DST_TABLE_INNER_JOIN_ON1 AND
    DST_TABLE_INNER_JOIN_ON2 AND
    dst._date_deleted IS NULL
    GROUP BY SRC_DST_GROUP_BY1,
    SRC_DST_GROUP_BY2
) src_dst ON SRC_DST_JOIN_ON1 AND
    SRC_DST_JOIN_ON2
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
        SRC_DST_SELECT
    ) src
    JOIN (SELECT * FROM src_catalog_name_src_collection_name_table WHERE (CHECK_VALID_GEO_SRC)) valid_src ON src._gobid = valid_src._gobid
    ARRAY_ELEMENTS
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst ON DST_TABLE_INNER_JOIN_ON1 AND
    DST_TABLE_INNER_JOIN_ON2 AND
    dst._date_deleted IS NULL
    GROUP BY SRC_DST_GROUP_BY1,
    SRC_DST_GROUP_BY2
) src_dst ON SRC_DST_JOIN_ON1 AND
    SRC_DST_JOIN_ON2
"""

        result = relater._src_dst_join()
        self.assertEqual(expected, result)

    def test_row_number_partition(self):
        relater = self._get_relater()
        relater._source_value_ref = lambda: 'SRC_VALUE_REF'
        relater.src_has_states = False

        expected = "row_number() OVER (PARTITION BY src._id,SRC_VALUE_REF ORDER BY SRC_VALUE_REF)"
        self.assertEqual(expected, relater._row_number_partition())

        relater.src_has_states = True
        expected = "row_number() OVER (PARTITION BY src._id,src.volgnummer,SRC_VALUE_REF ORDER BY SRC_VALUE_REF)"
        self.assertEqual(expected, relater._row_number_partition())

        expected = "row_number() OVER (PARTITION BY src._id,src.volgnummer,SRC_VALUE_REF_PARAM " \
                   "ORDER BY SRC_VALUE_REF_PARAM)"
        self.assertEqual(expected, relater._row_number_partition('SRC_VALUE_REF_PARAM'))

    def test_select_rest_src(self):
        relater = self._get_relater()
        relater._source_value_ref = lambda: "SRC_VAL_REF"

        relater.src_has_states = False
        relater.is_many = False
        self.assertEqual(f"""
    SELECT
        src.*,
        SRC_VAL_REF bronwaarde
    FROM src_catalog_name_src_collection_name_table src
    
    WHERE src._date_deleted IS NULL AND (_id) NOT IN (
        SELECT _id FROM src_entities
    ) AND SRC_VAL_REF IS NOT NULL
""", relater._select_rest_src())

        relater.src_has_states = True
        relater.is_many = True
        self.assertEqual(f"""
    SELECT
        src.*,
        SRC_VAL_REF bronwaarde
    FROM src_catalog_name_src_collection_name_table src
    JOIN jsonb_array_elements(src.src_field_name) json_arr_elm(item) ON json_arr_elm->>'bronwaarde' IS NOT NULL
    WHERE src._date_deleted IS NULL AND (_id,volgnummer) NOT IN (
        SELECT _id,volgnummer FROM src_entities
    )
""", relater._select_rest_src())

    def test_cleanup_tmp_tables(self):
        relater = self._get_relater()
        relater.src_intv_tmp_table = MagicMock()
        relater.dst_intv_tmp_table = MagicMock()

        relater._cleanup_tmp_tables()
        relater.src_intv_tmp_table.drop.assert_called_once()
        relater.dst_intv_tmp_table.drop.assert_called_once()

    @patch("gobupload.relate.update.logger", MagicMock())
    @patch("gobupload.relate.update._execute")
    def test_create_tmp_tables(self, mock_execute):
        relater = self._get_relater()
        relater._start_validity_per_seqnr = lambda tablename: "START_VALIDITIES_" + tablename
        relater.src_intv_tmp_table = MagicMock()
        relater.dst_intv_tmp_table = MagicMock()

        relater.src_table_name = 'src_table_name'
        relater.dst_table_name = 'dst_table_name'
        relater.src_has_states = True
        relater.dst_has_states = True

        relater._create_tmp_tables()

        relater.src_intv_tmp_table.create.assert_called_once()
        relater.dst_intv_tmp_table.create.assert_called_once()

        relater.src_intv_tmp_table.create.reset_mock()
        relater.dst_intv_tmp_table.create.reset_mock()
        relater.src_has_states = True
        relater.dst_has_states = False

        relater._create_tmp_tables()

        relater.src_intv_tmp_table.create.assert_called_once()
        relater.dst_intv_tmp_table.create.assert_not_called()

        relater.src_intv_tmp_table.create.reset_mock()
        relater.dst_intv_tmp_table.create.reset_mock()
        relater.src_has_states = False
        relater.dst_has_states = True

        relater._create_tmp_tables()

        relater.src_intv_tmp_table.create.assert_not_called()
        relater.dst_intv_tmp_table.create.assert_called_once()

        relater.src_table_name = 'src_table_name'
        relater.dst_table_name = 'src_table_name' # same table name, only create one tmp table

        relater.src_intv_tmp_table.create.reset_mock()
        relater.dst_intv_tmp_table.create.reset_mock()
        relater.src_has_states = True
        relater.dst_has_states = True

        # Should be different before
        self.assertNotEqual(relater.src_intv_tmp_table, relater.dst_intv_tmp_table)
        relater._create_tmp_tables()

        # But as src and dst table are the same, we set them to use the same intv tmp table
        self.assertEqual(relater.src_intv_tmp_table, relater.dst_intv_tmp_table)

        relater.src_intv_tmp_table.create.assert_called_once()
        # Don't check dst_intv_tmp_table, because that is the same object as src_intv_tmp_table now

    def test_changed_source_ids(self):
        relater = self._get_relater()
        expected = f"""
SELECT e.source_id
FROM events e
INNER JOIN jsonb_array_elements(e.contents -> 'modifications') modifications
ON modifications ->> 'key' IN ('begin_geldigheid', 'eind_geldigheid', '_source', 'attrA', 'attrB')
WHERE catalogue = 'CATALOG'
  AND entity = 'COLLECTION'
  AND eventid > LAST_EVENTID
  AND action = 'MODIFY'
UNION
SELECT e.source_id
FROM events e
WHERE catalogue = 'CATALOG'
  AND entity = 'COLLECTION'
  AND eventid > LAST_EVENTID
  AND action IN ('ADD', 'DELETE')
"""

        self.assertEqual(expected, relater._changed_source_ids('CATALOG', 'COLLECTION', 'LAST_EVENTID', ['attrA', 'attrB']))

    def test_src_entities(self):
        relater = self._get_relater()
        relater._source_value_ref = lambda: 'SOURCE_VAL_REF'
        relater._changed_source_ids = MagicMock(return_value='CHANGED_SRC_IDS')
        relater.relation_specs = [{'source_attribute': 'attrA'}, {'source_attribute': 'attrB'}, {}]
        relater.is_many = True
        expected = f"""
SELECT * FROM src_catalog_name_src_collection_name_table src
WHERE src._source_id IN (CHANGED_SRC_IDS)

"""
        self.assertEqual(expected, relater._src_entities())

        relater._changed_source_ids.assert_called_with(
            'src_catalog_name',
            'src_collection_name',
            "(SELECT COALESCE(MAX(_last_src_event), 0) FROM rel_src_catalog_name_src_collection_name_src_field_name)",
            ['src_field_name', 'attrA', 'attrB']
        )
        expected = f"""
SELECT * FROM src_catalog_name_src_collection_name_table src

ORDER BY src._last_event LIMIT 30000
"""
        self.assertEqual(expected, relater._src_entities(True))


        relater.is_many = False

        expected = f"""
SELECT * FROM src_catalog_name_src_collection_name_table src
WHERE SOURCE_VAL_REF IS NOT NULL AND src._source_id IN (CHANGED_SRC_IDS)

"""
        self.assertEqual(expected, relater._src_entities())
        relater.is_many = True

        relater.exclude_relation_table = True
        expected = f"""
SELECT * FROM src_catalog_name_src_collection_name_table src


"""
        self.assertEqual(expected, relater._src_entities())

        relater.exclude_relation_table = False
        relater.min_src_event_id = 204
        relater.max_src_event_id = 24904

        expected = f"""
SELECT * FROM src_catalog_name_src_collection_name_table src
WHERE src._last_event > 204 AND src._last_event <= 24904
ORDER BY src._last_event LIMIT 30000
"""
        self.assertEqual(expected, relater._src_entities(True))

    def test_dst_entities(self):
        relater = self._get_relater()
        relater._changed_source_ids = MagicMock(return_value='CHANGED_SOURCE_IDS')
        relater.relation_specs = [{'destination_attribute': 'attrA'}, {'destination_attribute': 'attrB'}]
        expected = f"SELECT * FROM dst_catalog_name_dst_collection_name_table WHERE _source_id IN (CHANGED_SOURCE_IDS)"
        self.assertEqual(expected, relater._dst_entities())
        relater._changed_source_ids.assert_called_with(
            'dst_catalog_name',
            'dst_collection_name',
            '(SELECT COALESCE(MAX(_last_dst_event), 0) FROM rel_src_catalog_name_src_collection_name_src_field_name)',
            ['attrA', 'attrB']
        )

        relater.exclude_relation_table = True
        expected = f"SELECT * FROM dst_catalog_name_dst_collection_name_table"
        self.assertEqual(expected, relater._dst_entities())

    def test_with_src_dst_entities(self):
        relater = self._get_relater()
        relater._src_entities = lambda initial: 'SRC_ENTITIES_INITIAL' if initial else 'SRC_ENTITIES'
        relater._dst_entities = lambda: 'DST_ENTITIES'

        self.assertEqual("""
-- All changed source entities
src_entities AS (SRC_ENTITIES)""", relater._with_src_entities())

        self.assertEqual("""
-- All changed source entities
src_entities AS (SRC_ENTITIES_INITIAL)""", relater._with_src_entities(True))

        self.assertEqual("""
-- All changed destination entities
dst_entities AS (DST_ENTITIES)""", relater._with_dst_entities())

    def test_with_max_src_event(self):
        relater = self._get_relater()
        self.assertTrue(
            "max_src_event AS (SELECT MAX(_last_event) _last_event FROM src_catalog_name_src_collection_name_table)"
            in relater._with_max_src_event()
        )

        relater.max_src_event_id = 284024

        self.assertTrue(
            "max_src_event AS (SELECT 284024 _last_event)" in relater._with_max_src_event()
        )

    def test_with_max_dst_event(self):
        relater = self._get_relater()
        self.assertTrue(
            "max_dst_event AS (SELECT MAX(_last_event) _last_event FROM dst_catalog_name_dst_collection_name_table)"
            in relater._with_max_dst_event()
        )

    def test_with_queries(self):
        relater = self._get_relater()
        relater._with_src_entities = lambda initial: 'SRC ENTITIES' + (' INITIAL' if initial else '')
        relater._with_dst_entities = lambda: 'DST ENTITIES'
        relater._with_max_src_event = lambda: 'MAX SRC EVENT'
        relater._with_max_dst_event = lambda: 'MAX DST EVENT'
        relater._start_validities = lambda: []
        self.assertTrue('WITH SRC ENTITIES,MAX SRC EVENT,MAX DST EVENT,DST ENTITIES' in relater._with_queries())

        self.assertTrue('WITH SRC ENTITIES INITIAL,MAX SRC EVENT,MAX DST EVENT' in relater._with_queries(True))

    def test_join_src_geldigheid(self):
        relater = self._get_relater()
        relater.src_table_name = 'src_table'
        relater.src_intv_tmp_table.name = 'src_intv_tmp_table_name'
        relater.src_has_states = False
        self.assertEqual("", relater._join_src_geldigheid())
        relater.src_has_states = True
        self.assertEqual("LEFT JOIN src_intv_tmp_table_name src_bg "
                         "ON src_bg._id = src._id AND src_bg.volgnummer = src.volgnummer",
                         relater._join_src_geldigheid())

    def test_join_dst_geldigheid(self):
        relater = self._get_relater()
        relater.dst_table_name = 'dst_table'
        relater.dst_intv_tmp_table.name = 'dst_intv_tmp_table_name'
        relater.dst_has_states = False
        self.assertEqual("", relater._join_dst_geldigheid())
        relater.dst_has_states = True
        self.assertEqual("LEFT JOIN dst_intv_tmp_table_name dst_bg "
                         "ON dst_bg._id = dst._id AND dst_bg.volgnummer = dst.volgnummer",
                         relater._join_dst_geldigheid())

    def test_join_max_event_ids(self):
        relater = self._get_relater()
        relater.src_table_name = 'SRC TABLE'
        relater.dst_table_name = 'DST TABLE'

        self.assertEqual(f"""
JOIN max_src_event ON TRUE
JOIN max_dst_event ON TRUE
""", relater._join_max_event_ids())

    def test_join_rel(self):
        relater = self._get_relater()
        relater.src_has_states = False

        self.assertEqual(f"""
LEFT JOIN (
    SELECT * FROM rel_src_catalog_name_src_collection_name_src_field_name
    WHERE src_id IN (SELECT _id FROM src_entities)
) rel ON rel.src_id = src._id AND src.src_field_name->>'bronwaarde' = rel.bronwaarde
     AND ((src._application = 'applicationA') OR (src._application = 'applicationB' AND rel.dst_id = dst._id))
""", relater._join_rel())

        relater.src_has_states = True

        self.assertEqual(f"""
LEFT JOIN (
    SELECT * FROM rel_src_catalog_name_src_collection_name_src_field_name
    WHERE (src_id, src_volgnummer) IN (SELECT _id, volgnummer FROM src_entities)
) rel ON rel.src_id = src._id AND rel.src_volgnummer = src.volgnummer
    AND src.src_field_name->>'bronwaarde' = rel.bronwaarde
     AND ((src._application = 'applicationA') OR (src._application = 'applicationB' AND rel.dst_id = dst._id))
""", relater._join_rel())

    def test_get_where(self):
        relater = self._get_relater()
        self.assertEqual(
            "WHERE (rel._date_deleted IS NULL OR src._id IS NOT NULL) AND dst._date_deleted IS NULL AND "
            "((src._application = 'applicationA') OR (src._application = 'applicationB' AND dst._id IS NOT NULL))",
            relater._get_where()
        )

        relater.exclude_relation_table = True
        self.assertEqual(
            "WHERE (src._id IS NOT NULL) AND dst._date_deleted IS NULL "
            "AND ((src._application = 'applicationA' AND row_number > 1)) "
            "AND ((src._application = 'applicationA') OR (src._application = 'applicationB' AND dst._id IS NOT NULL))",
            relater._get_where()
        )

    def test_switch_for_specs(self):
        relater = self._get_relater()
        relater.relation_specs = [
            {'some_attr': 'a', 'source': 'A'},
            {'some_attr': 'a', 'source': 'B'}
        ]

        # If attribute is equal over all specs, return a simplified version
        res = relater._switch_for_specs('some_attr', lambda spec: spec['some_attr'] * 3)
        self.assertEqual('aaa', res)

        relater.relation_specs = [
            {'some_attr': 'a', 'source': 'A'},
            {'some_attr': 'b', 'source': 'B'}
        ]

        # Attribute is not equal, return OR expression
        res = relater._switch_for_specs('some_attr', lambda spec: spec['some_attr'] * 3)
        self.assertEqual("((src._application = 'A' AND aaa) OR (src._application = 'B' AND bbb))", res)
        
        # One spec resolves to TRUE, simplify the TRUE.
        # Other spec resolves to FALSE, leave out application expression
        res = relater._switch_for_specs('some_attr', lambda spec: 'FALSE' if spec['some_attr'] != 'a' else 'TRUE')
        self.assertEqual("((src._application = 'A'))", res)

        # All specs resolve to FALSE. Simplify to a simple FALSE
        res = relater._switch_for_specs('some_attr', lambda spec: 'FALSE')
        self.assertEqual('FALSE', res)

    def _get_get_query_mocked_relater(self):
        relater = self._get_relater()
        relater._dst_table_outer_join_on = lambda: ['DST_TABLE_OUTER_JOIN_ON1', 'DST_TABLE_OUTER_JOIN_ON2']
        relater._select_expressions_src = lambda: ['SELECT_EXPRESSION1SRC', 'SELECT_EXPRESSION2SRC']
        relater._select_expressions_dst = lambda: ['SELECT_EXPRESSION1DST', 'SELECT_EXPRESSION2DST']
        relater._join_array_elements = lambda: 'ARRAY_ELEMENTS'
        relater._src_dst_join = lambda: 'SRC_DST_JOIN'
        relater._start_validities = lambda: 'SEQNR_BEGIN_GELDIGHEID'
        relater._join_src_geldigheid = lambda: 'JOIN_SRC_GELDIGHEID'
        relater._join_dst_geldigheid = lambda: 'JOIN_DST_GELDIGHEID'
        relater._with_queries = lambda x: 'WITH QUERIES'
        relater._get_where = lambda: 'WHERE CLAUSE'
        relater._join_rel = lambda: 'JOIN REL'
        relater._join_max_event_ids = lambda: 'JOIN MAX EVENTIDS'
        relater._select_rest_src = lambda: 'REST SRC'
        relater._select_aliases = lambda: ['SELECT_ALIAS1', 'SELECT_ALIAS2']
        relater._row_number_partition = lambda x: 'ROW_NUMBER_PARTITION(' + x + ')' if x else 'ROW_NUMBER_PARTITION'
        relater._src_dst_match = lambda x: ['SRC_DST_MATCH1(' + x + ')', 'SRC_DST_MATCH2(' + x + ')'] if x \
            else ['SRC_DST_MATCH1', 'SRC_DST_MATCH2']
        relater._union_deleted_relations = lambda src_or_dst: 'UNION_DELETED_' + src_or_dst

        return relater

    def test_get_query_singleref(self):
        relater = self._get_get_query_mocked_relater()
        relater.is_many = False

        expected = """
WITH QUERIES,
src_side AS (
    -- Relate all changed src entities
    SELECT
        SELECT_EXPRESSION1SRC,
    SELECT_EXPRESSION2SRC
    FROM src_entities src
    
    SRC_DST_JOIN
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst
        ON DST_TABLE_OUTER_JOIN_ON1 AND
    DST_TABLE_OUTER_JOIN_ON2
    JOIN REL
    JOIN_SRC_GELDIGHEID
    JOIN_DST_GELDIGHEID
    JOIN MAX EVENTIDS
    WHERE CLAUSE
)
,
dst_side AS (
    -- Relate all changed dst entities, but exclude relations that are also related in src_side
    SELECT
        SELECT_ALIAS1,
    SELECT_ALIAS2
    FROM (
    SELECT
        SELECT_EXPRESSION1DST,
    SELECT_EXPRESSION2DST,
        ROW_NUMBER_PARTITION(src.bronwaarde) AS row_number
    FROM dst_entities dst
    INNER JOIN (REST SRC) src
        ON SRC_DST_MATCH1(src.bronwaarde) AND
    SRC_DST_MATCH2(src.bronwaarde)
    LEFT JOIN rel_src_catalog_name_src_collection_name_src_field_name rel
        ON rel.src_id=src._id AND rel.src_volgnummer = src.volgnummer
        AND rel.src_source = src._source AND rel.bronwaarde = src.bronwaarde
         AND ((src._application = 'applicationA') OR (src._application = 'applicationB' AND rel.dst_id = dst._id))
    JOIN_SRC_GELDIGHEID
    JOIN_DST_GELDIGHEID
    JOIN MAX EVENTIDS
    WHERE CLAUSE
    ) q
)

-- All relations for changed src entities
SELECT * FROM src_side
UNION_DELETED_src
UNION ALL
-- All relations for changed dst entities
SELECT * FROM dst_side
UNION_DELETED_dst
"""

        result = relater.get_query()
        self.assertEqual(result, expected)

    def test_get_query_manyref(self):
        relater = self._get_get_query_mocked_relater()
        relater.is_many = True
        self.maxDiff = None

        expected = """
WITH QUERIES,
src_side AS (
    -- Relate all changed src entities
    SELECT
        SELECT_EXPRESSION1SRC,
    SELECT_EXPRESSION2SRC
    FROM src_entities src
    ARRAY_ELEMENTS
    SRC_DST_JOIN
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst
        ON DST_TABLE_OUTER_JOIN_ON1 AND
    DST_TABLE_OUTER_JOIN_ON2
    JOIN REL
    JOIN_SRC_GELDIGHEID
    JOIN_DST_GELDIGHEID
    JOIN MAX EVENTIDS
    WHERE CLAUSE
)
,
dst_side AS (
    -- Relate all changed dst entities, but exclude relations that are also related in src_side
    SELECT
        SELECT_ALIAS1,
    SELECT_ALIAS2
    FROM (
    SELECT
        SELECT_EXPRESSION1DST,
    SELECT_EXPRESSION2DST,
        ROW_NUMBER_PARTITION(src.bronwaarde) AS row_number
    FROM dst_entities dst
    INNER JOIN (REST SRC) src
        ON SRC_DST_MATCH1(src.bronwaarde) AND
    SRC_DST_MATCH2(src.bronwaarde)
    LEFT JOIN rel_src_catalog_name_src_collection_name_src_field_name rel
        ON rel.src_id=src._id AND rel.src_volgnummer = src.volgnummer
        AND rel.src_source = src._source AND rel.bronwaarde = src.bronwaarde
         AND ((src._application = 'applicationA') OR (src._application = 'applicationB' AND rel.dst_id = dst._id))
    JOIN_SRC_GELDIGHEID
    JOIN_DST_GELDIGHEID
    JOIN MAX EVENTIDS
    WHERE CLAUSE
    ) q
)

-- All relations for changed src entities
SELECT * FROM src_side
UNION_DELETED_src
UNION ALL
-- All relations for changed dst entities
SELECT * FROM dst_side
UNION_DELETED_dst
"""
        result = relater.get_query()
        self.assertEqual(result, expected)

    def test_get_conflicts_query(self):
        relater = self._get_get_query_mocked_relater()
        relater._prepare_query = MagicMock()
        relater.is_many = False

        expected = """
WITH QUERIES,
src_side AS (
    -- Relate all changed src entities
    SELECT
        SELECT_EXPRESSION1SRC,
    SELECT_EXPRESSION2SRC
    FROM src_entities src
    
    SRC_DST_JOIN
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst
        ON DST_TABLE_OUTER_JOIN_ON1 AND
    DST_TABLE_OUTER_JOIN_ON2
    
    JOIN_SRC_GELDIGHEID
    JOIN_DST_GELDIGHEID
    JOIN MAX EVENTIDS
    WHERE CLAUSE
)

-- All relations for changed src entities
SELECT * FROM src_side
"""
        result = relater.get_conflicts_query()
        self.assertEqual(result, expected)

    @patch("gobupload.relate.update._execute")
    def test_get_conflicts(self, mock_execute):
        relater = self._get_relater()
        relater._prepare_query = MagicMock()
        relater._cleanup = MagicMock()
        relater.get_conflicts_query = MagicMock()
        mock_execute.return_value = iter(['a', 'b', 'c'])

        result = list(relater.get_conflicts())
        self.assertEqual(['a', 'b', 'c'], result)
        mock_execute.assert_called_with(relater.get_conflicts_query.return_value, stream=True, max_row_buffer=25000)
        relater._prepare_query.assert_called_once()
        relater._cleanup.assert_called_once()

    def test_union_deleted_relations(self):
        relater = self._get_relater()
        relater._select_expressions_rel_delete = lambda: ['EXPRESSION1', 'EXPRESSION2']

        self.assertEqual("""
UNION ALL
-- Add all relations for entities in src_entities that should be deleted
-- These are all current relations that are referenced by src_entities but are not in src_side
-- anymore.
SELECT EXPRESSION1,
    EXPRESSION2
FROM rel_src_catalog_name_src_collection_name_src_field_name rel
WHERE (src_id, src_volgnummer) IN (SELECT _id, volgnummer FROM src_entities) AND rel.id NOT IN (SELECT rel_id FROM src_side WHERE rel_id IS NOT NULL)
    AND rel._date_deleted IS NULL
""", relater._union_deleted_relations('src'))

        self.assertEqual("""
UNION ALL
-- Add all relations for entities in dst_entities that should be deleted
-- These are all current relations that are referenced by dst_entities but are not in dst_side
-- anymore.
SELECT EXPRESSION1,
    EXPRESSION2
FROM rel_src_catalog_name_src_collection_name_src_field_name rel
WHERE dst_id IN (SELECT _id FROM dst_entities) AND rel.id NOT IN (SELECT rel_id FROM src_side WHERE rel_id IS NOT NULL UNION ALL SELECT rel_id FROM dst_side WHERE rel_id IS NOT NULL)
    AND rel._date_deleted IS NULL
""", relater._union_deleted_relations('dst'))

    def test_get_query_initial_load(self):
        relater = self._get_get_query_mocked_relater()
        relater.is_many = False

        expected = """
WITH QUERIES,
src_side AS (
    -- Relate all changed src entities
    SELECT
        SELECT_EXPRESSION1SRC,
    SELECT_EXPRESSION2SRC
    FROM src_entities src
    
    SRC_DST_JOIN
    LEFT JOIN dst_catalog_name_dst_collection_name_table dst
        ON DST_TABLE_OUTER_JOIN_ON1 AND
    DST_TABLE_OUTER_JOIN_ON2
    JOIN REL
    JOIN_SRC_GELDIGHEID
    JOIN_DST_GELDIGHEID
    JOIN MAX EVENTIDS
    WHERE CLAUSE
)

-- All relations for changed src entities
SELECT * FROM src_side
UNION_DELETED_src"""
        result = relater.get_query(True)
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

    @patch('gobupload.relate.update.ADD')
    @patch('gobupload.relate.update.MODIFY')
    @patch('gobupload.relate.update.DELETE')
    @patch('gobupload.relate.update.CONFIRM')
    def test_create_event(self, mock_confirm, mock_delete, mock_modify, mock_add):
        relater = self._get_relater()
        relater.dst_has_states = False

        # ADD event (rel row not present)
        row = {
            'rel_id': None,
            '_source_id': 'SOURCE ID',
            'a': 'val',
            'b': 'val',
            'rel_deleted': None,
        }
        event = relater._create_event(row)
        self.assertEqual(mock_add.create_event.return_value, event)
        mock_add.create_event.assert_called_with('SOURCE ID', 'SOURCE ID', {
            '_source_id': 'SOURCE ID',
            'a': 'val',
            'b': 'val',
        })

        # ADD event (rel row present, but previously deleted)
        row = {
            'rel_id': 'some existing rel id',
            '_source_id': 'SOURCE ID',
            'a': 'val',
            'b': 'val',
            'rel_deleted': 'some date',
            '_last_event': 'last event'
        }
        event = relater._create_event(row)
        self.assertEqual(mock_add.create_event.return_value, event)
        mock_add.create_event.assert_called_with('SOURCE ID', 'SOURCE ID', {
            '_source_id': 'SOURCE ID',
            'a': 'val',
            'b': 'val',
            '_last_event': 'last event'
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
            'rel_deleted': None,
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
            'rel_deleted': None,
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
            'rel_deleted': None,
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
            'rel_deleted': None,
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

    @patch("gobupload.relate.update._execute")
    def test_is_initial_load(self, mock_execute):
        relater = self._get_relater()

        mock_execute.return_value = iter([])
        self.assertTrue(relater._is_initial_load())

        mock_execute.return_value = iter([1])
        self.assertFalse(relater._is_initial_load())

    @patch("gobupload.relate.update._FORCE_FULL_RELATE_THRESHOLD", 1.0)
    def test_force_full_relate(self):
        relater = self._get_relater()
        relater._src_entities = MagicMock()
        relater._dst_entities = MagicMock()

        testcases = [
            # ([ total_src, total_dst, changed_src, changed_dst], result)
            ([100, 100, 50, 0], False),
            ([100, 100, 100, 0], True),
            ([100, 100, 0, 0], False),
            ([100, 100, 100, 100], True),
            ([100, 100, 50, 50], True),
            ([0, 100, 0, 0], True),
            ([100, 0, 0, 0], True),
        ]

        for counts, result in testcases:
            relater._get_count_for = MagicMock(side_effect=counts)

            self.assertEqual(result, relater._force_full_relate())

    def test_check_preconditions(self):
        relater = self._get_relater()
        relater.src_table_name = 'src_table'
        relater.src_catalog_name = 'src_catalog'
        relater.src_collection_name = 'src_collection'
        relater.src_field_name = 'src_field'
        relater._get_applications_in_src = MagicMock(return_value=['applicationA', 'applicationB'])
        relater.relation_specs = [{'source': 'applicationA'}, {'source': 'applicationB'}]

        # No Exceptions are thrown
        relater._check_preconditions()
        relater._get_applications_in_src.assert_called_once()

        # No Exceptions. Gobsources and src table don't match, but everything we need is defined -> Not all sources
        # have to be present in the src table
        relater._get_applications_in_src.return_value = ['applicationA']
        relater._check_preconditions()

        # Empty table. OK.
        relater._get_applications_in_src.return_value = []
        relater._check_preconditions()

        # applicationC is not defined in gobsources
        relater._get_applications_in_src.return_value = ['applicationC']
        with self.assertRaises(GOBException):
            relater._check_preconditions()

    @patch("gobupload.relate.update._execute")
    def test_get_max_src_event(self, mock_execute):
        relater = self._get_relater()
        relater.src_table_name = 'src_table'

        mock_execute.return_value = iter([(2480,)])

        self.assertEqual(2480, relater._get_max_src_event())
        mock_execute.assert_called_with("SELECT MAX(_last_event) FROM src_table")

    def test_get_paged_updates(self):
        relater = self._get_relater()
        relater._get_max_src_event = MagicMock(return_value=4024)
        relater._query_results = MagicMock(side_effect=[
            [{'src_last_event': 1}, {'src_last_event': 2}],
            [{'src_last_event': 3}, {'src_last_event': 4}],
            []
        ])

        result = list(relater._get_paged_updates())
        self.assertEqual([{'src_last_event': 1}, {'src_last_event': 2}, {'src_last_event': 3},
                          {'src_last_event': 4}], result)

        self.assertEqual(relater.max_src_event_id, 4024)
        self.assertEqual(relater.min_src_event_id, 4)
        self.assertEqual(3, relater._query_results.call_count)

    @patch("gobupload.relate.logger", MagicMock())
    def test_prepare_updates(self):
        relater = self._get_relater()
        relater._create_tmp_tables = MagicMock()
        relater._prepare_query()
        relater._create_tmp_tables.assert_called_once()

    @patch("gobupload.relate.logger", MagicMock())
    def test_cleanup(self):
        relater = self._get_relater()
        relater._cleanup_tmp_tables = MagicMock()
        relater._cleanup()
        relater._cleanup_tmp_tables.assert_called_once()

    @patch("gobupload.relate.logger", MagicMock())
    def test_get_updates(self):
        relater = self._get_relater()
        relater._prepare_query = MagicMock()
        relater._cleanup = MagicMock()
        relater._get_paged_updates = MagicMock(return_value=iter(['paged_update1', 'paged_update2']))
        relater._query_results = MagicMock(return_value=iter(['nonpaged_update1', 'nonpaged_update2']))

        self.assertEqual(['paged_update1', 'paged_update2'], list(relater._get_updates(True)))
        self.assertEqual(['nonpaged_update1', 'nonpaged_update2'], list(relater._get_updates(False)))

        relater._query_results.assert_called_with(False)
        relater._prepare_query.assert_called()
        relater._cleanup.assert_called()

    @patch("gobupload.relate.update._execute")
    def test_query_results(self, mock_execute):
        relater = self._get_relater()
        relater.get_query = MagicMock()
        mock_execute.return_value.__iter__.return_value = [{'a': 1}, {'b': 2}, {'c': 3}]

        self.assertEqual([{'a': 1}, {'b': 2}, {'c': 3}], list(relater._query_results('true_or_false')))

        relater.get_query.assert_called_with('true_or_false')

        # Assert re-raise exception
        mock_execute.return_value.__iter__.side_effect = Exception('mocked exception')
        with self.assertRaises(Exception):
            result = relater.update()

    @patch("gobupload.relate.update.EventCollector")
    @patch("gobupload.relate.update.ContentsWriter")
    @patch("gobupload.relate.update.ProgressTicker", MagicMock())
    def test_update(self, mock_contents_writer, mock_event_collector):
        relater = self._get_relater()
        relater._is_initial_load = MagicMock()
        relater._create_event = MagicMock(side_effect=lambda x: x)
        relater._get_updates = MagicMock(return_value=iter([{'a': 1}, {'b': 2}, {'c': 3}]))
        relater._format_relation = MagicMock(side_effect=lambda x: x)
        relater._check_preconditions = MagicMock()

        result = relater.update()

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

    def test_update_failing_precondition(self):
        relater = self._get_relater()
        relater._get_query = MagicMock()
        relater._check_preconditions = MagicMock(side_effect=Exception)

        with self.assertRaises(Exception):
            relater.update()

            # Make sure nothing is done yet. Check for preconditions is done first.
            relater._get_query.assert_not_called()
