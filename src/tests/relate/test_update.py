from unittest import TestCase
from unittest.mock import MagicMock, call, patch
from datetime import date, datetime

from gobcore.exceptions import GOBException
from gobcore.model.metadata import FIELD

from gobupload.relate.update import EventCreator, RelateException, Relater
from gobupload.storage.handler import StreamSession


class MockModel:
    _has_states = {
        'src_collection_name': True,
        'src_many_collection_name': True,
        'dst_collection_name': False,
    }

    def __getitem__(self, cat):
        if cat == "src_catalog_name":
            return {
                'collections': {
                    'src_collection_name': {
                        'all_fields': {
                            'src_field_name': {
                                'type': 'GOB.Reference',
                                'ref': 'dst_catalog_name:dst_collection_name',
                                }
                            },
                        'abbreviation': 'srcabbr'
                    },
                    'src_many_collection_name': {
                        'all_fields': {
                            'src_field_name': {
                                'type': 'GOB.ManyReference',
                                'ref': 'dst_catalog_name:dst_collection_name',
                                }
                            },
                        'abbreviation': 'abbr'
                    }
                }
            }
        elif cat == "dst_catalog_name":
            return {
                'collections': {
                    'dst_collection_name': {
                        'abbreviation': 'dstabbr'
                    }
                }
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



class TestEventCreator(TestCase):
    def test_get_modifications(self):
        ec = EventCreator(False)

        row = {
            'rel_a': 'a field',
            'rel_b': 'b field',
            'rel_c': 'c field',
            'a': 'a field',
            'b': 'b field',
            'c': 'changed field',
        }

        self.assertEqual([], ec._get_modifications(row, ['a', 'b']))

        self.assertEqual([
            {
                'old_value': 'c field',
                'new_value': 'changed field',
                'key': 'c'
            }
        ], ec._get_modifications(row, ['a', 'b', 'c']))

    @patch('gobupload.relate.update._RELATE_VERSION', '123.0')
    @patch('gobupload.relate.update.ADD')
    @patch('gobupload.relate.update.MODIFY')
    @patch('gobupload.relate.update.DELETE')
    def test_create_event(self, mock_delete, mock_modify, mock_add):
        ec = EventCreator(False)

        # ADD event (rel row not present)
        row = {
            'id': 'the_id',
            'rel_id': None,
            '_source_id': 'SOURCE ID',
            '_id': 'the_id',
            'a': 'val',
            'b': 'val',
            'rel_deleted': None,
        }
        event = ec.create_event(row)
        self.assertEqual(mock_add.create_event.return_value, event)
        mock_add.create_event.assert_called_with('the_id', {
            'id': 'the_id',
            '_source_id': 'SOURCE ID',
            '_id': 'the_id',
            'a': 'val',
            'b': 'val',
        }, '123.0')

        # ADD event (rel row present, but previously deleted)
        row = {
            'id': 'the_id',
            'rel_id': 'some existing rel id',
            '_source_id': 'SOURCE ID',
            '_id': 'the_id',
            'a': 'val',
            'b': 'val',
            'rel_deleted': 'some date',
            '_last_event': 'last event'
        }
        event = ec.create_event(row)
        self.assertEqual(mock_add.create_event.return_value, event)
        mock_add.create_event.assert_called_with('the_id', {
            'id': 'the_id',
            '_source_id': 'SOURCE ID',
            '_id': 'the_id',
            'a': 'val',
            'b': 'val',
            '_last_event': 'last event'
        }, '123.0')

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
        event = ec.create_event(row)
        self.assertEqual(mock_delete.create_event.return_value, event)
        mock_delete.create_event.assert_called_with('rel id', {'_last_event': 'last'}, '123.0')

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
        event = ec.create_event(row)
        self.assertEqual(mock_delete.create_event.return_value, event)
        mock_delete.create_event.assert_called_with('rel id', {'_last_event': 'last'}, '123.0')

        # MODIFY EVENT (hash differs, and modifications detected)
        ec._get_hash = lambda x: 'THE HASH'
        ec._get_modifications = lambda a, b: ['a']
        row = {
            'id': 'the_id',
            'rel_id': 'the_id',
            'src_deleted': None,
            'src_id': 'src id',
            'rel__hash': 'DIFFERENT HASH',
            '_last_event': 'last',
            'rel_deleted': None,
        }
        event = ec.create_event(row)
        self.assertEqual(mock_modify.create_event.return_value, event)
        mock_modify.create_event.assert_called_with('the_id', {
            'modifications': ['a'],
            '_last_event': 'last',
            '_hash': 'THE HASH'
        }, '123.0')

        # CONFIRM EVENT = None
        row = {
            'id': 'rel id',
            'rel_id': 'rel id',
            'src_deleted': None,
            'src_id': 'src id',
            'rel__hash': 'THE HASH',
            '_last_event': 'last',
            'rel_deleted': None,
        }
        event = ec.create_event(row)
        self.assertIsNone(event)


@patch("gobupload.relate.update.logger", MagicMock())
@patch("gobupload.relate.update.Relater.model", MockModel())
@patch("gobupload.relate.update.Relater.sources", MockSources())
@patch("gobupload.relate.update.get_relation_name", lambda m, cat, col, field: f"{cat}_{col}_{field}")
class TestRelaterInit(TestCase):

    def setUp(self) -> None:
        self.mock_session = MagicMock(spec=StreamSession)
        self.catalog = "src_catalog_name"
        self.collection = "src_collection_name"
        self.field_name = "src_field_name"

        self.mock_session.execute.return_value.scalars.return_value.all.return_value = ["applicationA"]
        self.relater = lambda: Relater(self.mock_session, self.catalog, self.collection, self.field_name)

    def test_init(self):
        relater = self.relater()

        assert relater.src_catalog_name == self.catalog
        assert relater.src_collection_name == self.collection
        assert relater.src_field_name == self.field_name

        src_collection_name = MockModel()["src_catalog_name"]["collections"]["src_collection_name"]
        assert src_collection_name == relater.src_collection
        assert src_collection_name["all_fields"]["src_field_name"] == relater.src_field

        assert "dst_catalog_name_dst_collection_name_table_clone" == relater.dst_table_name
        assert relater.src_has_states is True
        assert relater.dst_has_states is False

        # applicationB should be filtered out
        assert [{"source": "applicationA", "multiple_allowed": False}] == relater.relation_specs
        assert relater.is_many is False
        assert "rel_src_catalog_name_src_collection_name_src_field_name_clone" == relater.relation_table

        query = "SELECT DISTINCT _application FROM src_catalog_name_src_collection_name_table_clone"
        self.mock_session.execute.assert_any_call(query)

        assert "src_catalog_name_srcabbr_src_field_name_result" == relater.result_table_name

    def test_init_relate_exception(self):
        with patch("gobupload.relate.update.Relater.sources.get_field_relations", lambda cat, col, field: []):
            self.assertRaises(RelateException, self.relater)

    def test_init_many(self):
        relater = self.relater()
        assert relater.is_many is False

        relater = Relater(MagicMock(), "src_catalog_name", "src_many_collection_name", "src_field_name")
        assert relater.is_many is True


@patch("gobupload.relate.update.logger", MagicMock())
@patch("gobupload.relate.update.Relater.model", MockModel())
@patch("gobupload.relate.update.Relater.sources", MockSources())
@patch("gobupload.relate.update.Relater._get_applications_in_src", lambda *args: ['applicationA', 'applicationB'])
@patch("gobupload.relate.update.get_relation_name", lambda m, cat, col, field: f"{cat}_{col}_{field}")
class TestRelater(TestCase):

    def _get_relater(self):
        return Relater(MagicMock(spec=StreamSession), 'src_catalog_name', 'src_collection_name', 'src_field_name')

    def test_validity_select_expressions_src(self):
        test_cases = [
            (True, True, ('GREATEST(src.begin_geldigheid, dst.begin_geldigheid, PROVIDED_START_VALIDITY)',
                          'LEAST(src.eind_geldigheid, dst.eind_geldigheid)')),
            (True, False, ('GREATEST(src.begin_geldigheid, PROVIDED_START_VALIDITY)', 'src.eind_geldigheid')),
            (False, True, ('GREATEST(dst.begin_geldigheid, PROVIDED_START_VALIDITY)', 'dst.eind_geldigheid')),
            (False, False, ('PROVIDED_START_VALIDITY', 'NULL')),
        ]

        relater = self._get_relater()
        relater._provided_start_validity = MagicMock(return_value='PROVIDED_START_VALIDITY')

        for src_has_states, dst_has_states, result in test_cases:
            relater.src_has_states = src_has_states
            relater.dst_has_states = dst_has_states
            self.assertEqual(result, relater._validity_select_expressions_src())

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

        self.assertEqual([
            'a',
            'src_volgnummer',
            'dst_volgnummer',
        ], relater._select_aliases(True))

    def test_build_select_expressions(self):
        relater = self._get_relater()
        relater._select_aliases = lambda x: ['a', 'b']

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
        relater._validity_select_expressions_src = lambda: ('SRC_START_VALIDITY', 'SRC_END_VALIDITY')
        result = relater._select_expressions_src(123, 456)
        self.assertEqual(relater._build_select_expressions.return_value, result)

    def test_select_expressions_rel_delete(self):
        relater = self._get_relater()
        res = relater._select_expressions_rel_delete()
        expected = [
            'NULL AS _version',
            'NULL AS _application',
            'NULL AS _source_id',
            'NULL AS _source',
            'NULL::timestamp without time zone AS _expiration_date',
            'NULL AS _id',
            'NULL AS _tid',
            'NULL AS id',
            'NULL AS derivation',
            'NULL AS src_source',
            'NULL AS src_id',
            'NULL::integer AS src_last_event',
            'NULL AS dst_source',
            'NULL AS dst_id',
            'NULL AS bronwaarde',
            'NULL::integer AS _last_src_event',
            'NULL::integer AS _last_dst_event',
            'NULL::timestamp without time zone AS begin_geldigheid',
            'NULL::timestamp without time zone AS eind_geldigheid',
            'NULL::timestamp without time zone AS src_deleted',
            'NULL::integer AS row_number',
            'rel._last_event AS _last_event',
            'NULL::timestamp without time zone AS rel_deleted',
            'rel.id AS rel_id',
            'rel.dst_id AS rel_dst_id',
            'rel.dst_volgnummer AS rel_dst_volgnummer',
            'rel._expiration_date AS rel__expiration_date',
            'rel.begin_geldigheid AS rel_begin_geldigheid',
            'rel.eind_geldigheid AS rel_eind_geldigheid',
            'rel._hash AS rel__hash',
            'NULL::integer AS src_volgnummer'
        ]

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
        self.assertEqual("src._id || '.' || src.volgnummer || '.' || src._source || '.' || (BRONWAARDE)",
                         relater._get_id())

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

    def test_provided_start_validity(self):
        relater = self._get_relater()
        relater.is_many = False

        self.assertEqual("(src.src_field_name->>'begin_geldigheid')::timestamp without time zone",
                         relater._provided_start_validity())

        relater.is_many = True
        self.assertEqual("(json_arr_elm.item->>'begin_geldigheid')::timestamp without time zone",
                         relater._provided_start_validity())

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
            "src_dst.dst_id IS NOT NULL",
        ]
        self.assertEqual(expected, relater._src_dst_join_on())

        relater.src_has_states = True
        expected = [
            "src_dst.src_id = src._id",
            "src_dst.src_volgnummer = src.volgnummer",
            "src_dst._source = src._source",
            "src_dst.bronwaarde = json_obj_ref->>'bronwaarde'",
            "src_dst.dst_id IS NOT NULL",
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
INNER JOIN (
    SELECT
        SRC_DST SELECT EXPRESSION1,
    SRC_DST SELECT EXPRESSION2
    FROM (
        SELECT * FROM src_entities WHERE _date_deleted IS NULL
    ) src
    
    
    LEFT JOIN (DST ENTITIES) dst ON DST_TABLE_INNER_JOIN_ON1 AND
    DST_TABLE_INNER_JOIN_ON2 AND
    dst._date_deleted IS NULL
    GROUP BY SRC_DST_GROUP_BY1,
    SRC_DST_GROUP_BY2
) src_dst ON SRC_DST_JOIN_ON1 AND
    SRC_DST_JOIN_ON2
"""

        result = relater._src_dst_join('DST ENTITIES')
        self.assertEqual(expected, result)

    def test_src_dst_join_no_geo_manyref(self):
        relater = self._get_src_dst_join_mocked_relater()
        relater._have_geo_specs = lambda: False
        relater.is_many = True

        expected = """
INNER JOIN (
    SELECT
        SRC_DST SELECT EXPRESSION1,
    SRC_DST SELECT EXPRESSION2
    FROM (
        SELECT * FROM src_entities WHERE _date_deleted IS NULL
    ) src
    
    ARRAY_ELEMENTS
    LEFT JOIN (DST ENTITIES) dst ON DST_TABLE_INNER_JOIN_ON1 AND
    DST_TABLE_INNER_JOIN_ON2 AND
    dst._date_deleted IS NULL
    GROUP BY SRC_DST_GROUP_BY1,
    SRC_DST_GROUP_BY2
) src_dst ON SRC_DST_JOIN_ON1 AND
    SRC_DST_JOIN_ON2
"""

        result = relater._src_dst_join("DST ENTITIES")
        self.assertEqual(expected, result)

    def test_src_dst_join_geo_singleref(self):
        relater = self._get_src_dst_join_mocked_relater()
        relater._have_geo_specs = lambda: True
        relater.is_many = False

        expected = """
INNER JOIN (
    SELECT
        SRC_DST SELECT EXPRESSION1,
    SRC_DST SELECT EXPRESSION2
    FROM (
        SELECT * FROM src_entities WHERE _date_deleted IS NULL
    ) src
    JOIN (SELECT * FROM src_catalog_name_src_collection_name_table_clone WHERE (CHECK_VALID_GEO_SRC)) valid_src ON src._gobid = valid_src._gobid
    
    LEFT JOIN (DST ENTITIES) dst ON DST_TABLE_INNER_JOIN_ON1 AND
    DST_TABLE_INNER_JOIN_ON2 AND
    dst._date_deleted IS NULL
    GROUP BY SRC_DST_GROUP_BY1,
    SRC_DST_GROUP_BY2
) src_dst ON SRC_DST_JOIN_ON1 AND
    SRC_DST_JOIN_ON2
"""

        result = relater._src_dst_join("DST ENTITIES")
        self.assertEqual(expected, result)

    def test_src_dst_join_geo_manyref(self):
        relater = self._get_src_dst_join_mocked_relater()
        relater._have_geo_specs = lambda: True
        relater.is_many = True

        expected = """
INNER JOIN (
    SELECT
        SRC_DST SELECT EXPRESSION1,
    SRC_DST SELECT EXPRESSION2
    FROM (
        SELECT * FROM src_entities WHERE _date_deleted IS NULL
    ) src
    JOIN (SELECT * FROM src_catalog_name_src_collection_name_table_clone WHERE (CHECK_VALID_GEO_SRC)) valid_src ON src._gobid = valid_src._gobid
    ARRAY_ELEMENTS
    LEFT JOIN (DST ENTITIES) dst ON DST_TABLE_INNER_JOIN_ON1 AND
    DST_TABLE_INNER_JOIN_ON2 AND
    dst._date_deleted IS NULL
    GROUP BY SRC_DST_GROUP_BY1,
    SRC_DST_GROUP_BY2
) src_dst ON SRC_DST_JOIN_ON1 AND
    SRC_DST_JOIN_ON2
"""

        result = relater._src_dst_join("DST ENTITIES")
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
        relater._provided_start_validity = lambda: "PROVIDED_START_VALIDITY"

        relater.src_has_states = False
        relater.is_many = False
        self.assertEqual(f"""
    SELECT
        src.*,
        SRC_VAL_REF bronwaarde,
        PROVIDED_START_VALIDITY provided_begin_geldigheid
    FROM src_catalog_name_src_collection_name_table_clone src
    
    WHERE src._date_deleted IS NULL AND (_id) NOT IN (
        SELECT _id FROM src_entities
    ) AND SRC_VAL_REF IS NOT NULL
""", relater._select_rest_src())

        relater.src_has_states = True
        relater.is_many = True
        self.assertEqual(f"""
    SELECT
        src.*,
        SRC_VAL_REF bronwaarde,
        PROVIDED_START_VALIDITY provided_begin_geldigheid
    FROM src_catalog_name_src_collection_name_table_clone src
    JOIN jsonb_array_elements(src.src_field_name) json_arr_elm(item) ON json_arr_elm->>'bronwaarde' IS NOT NULL
    WHERE src._date_deleted IS NULL AND (_id,volgnummer) NOT IN (
        SELECT _id,volgnummer FROM src_entities
    )
""", relater._select_rest_src())

    def test_create_tmp_tables(self):
        relater = self._get_relater()
        relater._start_validity_per_seqnr = lambda tablename: "START_VALIDITIES_" + tablename
        relater._create_tmp_result_table = MagicMock()

        relater.src_table_name = 'src_table_name'
        relater.dst_table_name = 'dst_table_name'
        relater.src_has_states = True
        relater.dst_has_states = True

        with relater:
            pass

        relater._create_tmp_result_table.assert_called_once()

        relater._create_tmp_result_table.reset_mock()
        relater.src_has_states = True
        relater.dst_has_states = False

        with relater:
            pass

        relater._create_tmp_result_table.assert_called_once()

        relater._create_tmp_result_table.reset_mock()
        relater.src_has_states = False
        relater.dst_has_states = True

        with relater:
            pass

        relater._create_tmp_result_table.assert_called_once()

        relater.src_table_name = 'src_table_name'
        relater.dst_table_name = 'src_table_name'  # same table name, only create one tmp table

        relater._create_tmp_result_table.reset_mock()
        relater.src_has_states = True
        relater.dst_has_states = True

        with relater:
            pass

        relater._create_tmp_result_table.assert_called_once()

    def test_create_tmp_result_table(self):
        relater = self._get_relater()
        relater.result_table_name = "tmp_table_name"
        relater.dst_has_states = True
        relater.src_has_states = True
        relater._create_tmp_result_table()

        query = """CREATE TEMPORARY TABLE tmp_table_name (
    rowid serial,
    _version varchar,
    _application varchar,
    _source_id varchar,
    _source varchar,
    _expiration_date timestamp,
    _id varchar,
    _tid varchar,
    id varchar,
    derivation varchar,
    src_source varchar,
    src_id varchar,
    src_last_event integer,
    dst_source varchar,
    dst_id varchar,
    bronwaarde varchar,
    _last_src_event integer,
    _last_dst_event integer,
    begin_geldigheid timestamp,
    eind_geldigheid timestamp,
    src_deleted timestamp,
    row_number integer,
    _last_event integer,
    rel_deleted timestamp,
    rel_id varchar,
    rel_dst_id varchar,
    rel_dst_volgnummer integer,
    rel__expiration_date timestamp,
    rel_begin_geldigheid timestamp,
    rel_eind_geldigheid timestamp,
    rel__hash varchar,
    src_volgnummer integer,
    dst_volgnummer integer
)"""

        relater.session.execute.assert_called_with(query)

    def test_src_entities_range(self):
        relater = self._get_relater()
        relater._source_value_ref = lambda: 'SOURCE_VAL_REF'
        relater.is_many = True
        expected = f"""
SELECT * FROM src_catalog_name_src_collection_name_table_clone src

"""
        self.assertEqual(expected, relater._src_entities_range(0))

        relater.is_many = False
        expected = f"""
SELECT * FROM src_catalog_name_src_collection_name_table_clone src
WHERE SOURCE_VAL_REF IS NOT NULL
"""
        self.assertEqual(expected, relater._src_entities_range(0))

        expected = f"""
SELECT * FROM src_catalog_name_src_collection_name_table_clone src
WHERE SOURCE_VAL_REF IS NOT NULL AND src._last_event > 1 AND src._last_event <= 100
"""
        self.assertEqual(expected, relater._src_entities_range(1, 100))

    def test_dst_entities_range(self):
        relater = self._get_relater()

        expected = f"""
SELECT * FROM dst_catalog_name_dst_collection_name_table_clone dst

"""
        self.assertEqual(expected, relater._dst_entities_range(0))

        expected = f"""
SELECT * FROM dst_catalog_name_dst_collection_name_table_clone dst
WHERE dst._last_event > 1 AND dst._last_event <= 100
"""
        self.assertEqual(expected, relater._dst_entities_range(1, 100))

    def test_join_rel(self):
        relater = self._get_relater()
        relater.src_has_states = False

        self.assertEqual("LEFT JOIN rel_src_catalog_name_src_collection_name_src_field_name_clone rel"
                         " ON rel.src_id = src._id"
                         " AND rel.src_source = src._source"
                         " AND src.src_field_name->>'bronwaarde' = rel.bronwaarde"
                         " AND ((src._application = 'applicationA') OR (src._application = 'applicationB'"
                         " AND rel.dst_id = dst._id))", relater._join_rel())

        relater.src_has_states = True

        self.assertEqual("LEFT JOIN rel_src_catalog_name_src_collection_name_src_field_name_clone rel"
                         " ON rel.src_id = src._id"
                         " AND rel.src_volgnummer = src.volgnummer"
                         " AND rel.src_source = src._source"
                         " AND src.src_field_name->>'bronwaarde' = rel.bronwaarde"
                         " AND ((src._application = 'applicationA') OR"
                         " (src._application = 'applicationB' AND rel.dst_id = dst._id))"
                         , relater._join_rel())

    def test_get_where(self):
        relater = self._get_relater()
        self.assertEqual(
            "WHERE (rel._date_deleted IS NULL OR src._id IS NOT NULL) AND dst._date_deleted IS NULL AND "
            "((src._application = 'applicationA') OR (src._application = 'applicationB' AND dst._id IS NOT NULL))",
            relater._get_where()
        )

        self.assertEqual(
            "WHERE (src._id IS NOT NULL) AND dst._date_deleted IS NULL "
            "AND ((src._application = 'applicationA' AND row_number > 1)) "
            "AND ((src._application = 'applicationA') OR (src._application = 'applicationB' AND dst._id IS NOT NULL))",
            relater._get_where(True)
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

    def test_create_delete_events_query(self):
        relater = self._get_relater()
        relater._src_entities_range = lambda min, max: f"SRC ENTITIES {min}-{max}"
        relater._dst_entities_range = lambda min, max: f"DST ENTITIES {min}-{max}"
        relater._select_expressions_rel_delete = MagicMock(return_value=['EXPR1', 'EXPR2'])

        # Test 1
        relater.src_has_states = True
        relater.dst_has_states = True

        expected = f"""
SELECT EXPR1,
    EXPR2
FROM rel_src_catalog_name_src_collection_name_src_field_name_clone rel
WHERE rel.id IN (
    SELECT rel.id FROM rel_src_catalog_name_src_collection_name_src_field_name_clone rel
    JOIN src_catalog_name_src_collection_name_table_clone src ON rel.src_id = src._id AND rel.src_volgnummer = src.volgnummer
      AND src._last_event > 1 AND src._last_event <= 2
    LEFT JOIN src_catalog_name_srcabbr_src_field_name_result res ON res.rel_id = rel.id
    WHERE rel._date_deleted IS NULL AND res.rel_id IS NULL
    UNION
    SELECT rel.id FROM rel_src_catalog_name_src_collection_name_src_field_name_clone rel
    JOIN dst_catalog_name_dst_collection_name_table_clone dst ON rel.dst_id = dst._id AND rel.dst_volgnummer = dst.volgnummer
      AND dst._last_event > 3 AND dst._last_event <= 4
    LEFT JOIN src_catalog_name_srcabbr_src_field_name_result res ON res.rel_id = rel.id
    WHERE rel._date_deleted IS NULL AND res.rel_id IS NULL
)
"""
        self.assertEqual(expected, relater._create_delete_events_query(1, 2, 3, 4))

        # Test 2. Switch states
        relater.src_has_states = False
        relater.dst_has_states = False

        expected = f"""
SELECT EXPR1,
    EXPR2
FROM rel_src_catalog_name_src_collection_name_src_field_name_clone rel
WHERE rel.id IN (
    SELECT rel.id FROM rel_src_catalog_name_src_collection_name_src_field_name_clone rel
    JOIN src_catalog_name_src_collection_name_table_clone src ON rel.src_id = src._id
      AND src._last_event > 1 AND src._last_event <= 2
    LEFT JOIN src_catalog_name_srcabbr_src_field_name_result res ON res.rel_id = rel.id
    WHERE rel._date_deleted IS NULL AND res.rel_id IS NULL
    UNION
    SELECT rel.id FROM rel_src_catalog_name_src_collection_name_src_field_name_clone rel
    JOIN dst_catalog_name_dst_collection_name_table_clone dst ON rel.dst_id = dst._id
      AND dst._last_event > 3 AND dst._last_event <= 4
    LEFT JOIN src_catalog_name_srcabbr_src_field_name_result res ON res.rel_id = rel.id
    WHERE rel._date_deleted IS NULL AND res.rel_id IS NULL
)
"""
        self.assertEqual(expected, relater._create_delete_events_query(1, 2, 3, 4))

    def _get_get_query_mocked_relater(self):
        relater = self._get_relater()
        relater._dst_table_outer_join_on = lambda: ['DST_TABLE_OUTER_JOIN_ON1', 'DST_TABLE_OUTER_JOIN_ON2']
        relater._select_expressions_src = lambda max_src, max_dst, conflicts: [f'SELECT_EXPRESSION1SRC{max_src}', f'SELECT_EXPRESSION2SRC{max_dst}', 'CONFLICTS' if conflicts else 'FULL']
        relater._select_expressions_dst = lambda: ['SELECT_EXPRESSION1DST', 'SELECT_EXPRESSION2DST']
        relater._join_array_elements = lambda: 'ARRAY_ELEMENTS'
        relater._src_dst_join = lambda dst_entities: 'SRC_DST_JOIN ' + dst_entities
        relater._get_where = lambda is_conflicts: 'WHERE CLAUSE ' + ("CONFLICTS" if is_conflicts else "FULL")
        relater._join_rel = lambda: 'JOIN REL'

        return relater

    def test_get_query_singleref(self):
        relater = self._get_get_query_mocked_relater()
        relater.is_many = False

        expected = """
WITH
src_entities AS (SRC ENTITIES)
SELECT
    SELECT_EXPRESSION1SRC50,
    SELECT_EXPRESSION2SRC100,
    FULL
FROM src_entities src

SRC_DST_JOIN DST ENTITIES
LEFT JOIN dst_catalog_name_dst_collection_name_table_clone dst ON DST_TABLE_OUTER_JOIN_ON1 AND
    DST_TABLE_OUTER_JOIN_ON2
JOIN REL
WHERE CLAUSE FULL
"""

        result = relater.get_query("SRC ENTITIES", "DST ENTITIES", 50, 100)
        self.assertEqual(result, expected)

    def test_get_query_manyref(self):
        relater = self._get_get_query_mocked_relater()
        relater.is_many = True
        self.maxDiff = None

        expected = """
WITH
src_entities AS (SRC ENTITIES)
SELECT
    SELECT_EXPRESSION1SRC50,
    SELECT_EXPRESSION2SRC100,
    FULL
FROM src_entities src
ARRAY_ELEMENTS
SRC_DST_JOIN DST ENTITIES
LEFT JOIN dst_catalog_name_dst_collection_name_table_clone dst ON DST_TABLE_OUTER_JOIN_ON1 AND
    DST_TABLE_OUTER_JOIN_ON2
JOIN REL
WHERE CLAUSE FULL
"""
        result = relater.get_query("SRC ENTITIES", "DST ENTITIES", 50, 100)
        self.assertEqual(result, expected)

    def test_get_query_conflicts_query(self):
        relater = self._get_get_query_mocked_relater()
        relater.is_many = False

        expected = """
WITH
src_entities AS (SRC ENTITIES)
SELECT
    SELECT_EXPRESSION1SRC50,
    SELECT_EXPRESSION2SRC100,
    CONFLICTS
FROM src_entities src

SRC_DST_JOIN DST ENTITIES
LEFT JOIN dst_catalog_name_dst_collection_name_table_clone dst ON DST_TABLE_OUTER_JOIN_ON1 AND
    DST_TABLE_OUTER_JOIN_ON2

WHERE CLAUSE CONFLICTS
"""
        result = relater.get_query("SRC ENTITIES", "DST ENTITIES", 50, 100, True)
        self.assertEqual(result, expected)

    def test_get_full_query(self):
        relater = self._get_relater()
        relater._get_max_src_event = MagicMock()
        relater._get_max_dst_event = MagicMock()
        relater._src_entities_range = MagicMock()
        relater._dst_entities_range = MagicMock()
        relater.get_query = MagicMock()

        self.assertEqual(relater.get_query.return_value, relater.get_full_query('TrueOrFalse'))
        relater.get_query.assert_called_with(
            relater._src_entities_range.return_value,
            relater._dst_entities_range.return_value,
            relater._get_max_src_event.return_value,
            relater._get_max_dst_event.return_value,
            'TrueOrFalse',
        )

        relater._src_entities_range.assert_called_with(0, relater._get_max_src_event.return_value)
        relater._dst_entities_range.assert_called_with()

    def test_format_relation(self):
        rows = [
            {FIELD.START_VALIDITY: None, FIELD.END_VALIDITY: None},
            {'one': 1, 'two': 2},
            {FIELD.START_VALIDITY: date(2020, 1, 30), FIELD.END_VALIDITY: datetime(2020, 2, 27, 13, 27, 21)},
        ]

        expected_result = [
            {FIELD.START_VALIDITY: None, FIELD.END_VALIDITY: None},
            {'one': 1, 'two': 2},
            {FIELD.START_VALIDITY: datetime(2020, 1, 30, 0, 0), FIELD.END_VALIDITY: datetime(2020, 2, 27, 13, 27, 21)},
        ]

        updater = self._get_relater()
        self.assertEqual(expected_result, [updater._format_relation(relation) for relation in rows])

    def test_is_initial_load(self):
        relater = self._get_relater()

        relater.session.execute.return_value.scalar.return_value = None
        assert relater._is_initial_load() is True

        relater.session.execute.return_value.scalar.return_value = 1
        assert relater._is_initial_load() is False

    @patch("gobupload.relate.update._FORCE_FULL_RELATE_THRESHOLD", 1.0)
    def test_force_full_relate(self):
        relater = self._get_relater()
        relater._get_changed_ranges = MagicMock(return_value=(1, 2, 3, 4))
        relater._src_entities_range = MagicMock()
        relater._dst_entities_range = MagicMock()

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
            relater.session.execute.return_value.scalar.side_effect = counts

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

    def test_get_max_src_event(self):
        relater = self._get_relater()
        relater.src_table_name = 'src_table'
        relater.session.execute.return_value.scalar.return_value = 2480

        assert 2480 == relater._get_max_src_event()
        relater.session.execute.assert_called_with("SELECT MAX(_last_event) FROM src_table")

    def test_get_max_dst_event(self):
        relater = self._get_relater()
        relater.dst_table_name = 'dst_table'

        relater.session.execute.return_value.scalar.return_value = 2480

        assert 2480 == relater._get_max_dst_event()
        relater.session.execute.assert_called_with("SELECT MAX(_last_event) FROM dst_table")

    def test_get_min_src_event(self):
        relater = self._get_relater()
        relater.relation_table = 'rel_table'

        relater.session.execute.return_value.scalar.return_value = 2480

        assert 2480 == relater._get_min_src_event()
        relater.session.execute.assert_called_with("SELECT MAX(_last_src_event) FROM rel_table")

    def test_get_min_dst_event(self):
        relater = self._get_relater()
        relater.relation_table = 'rel_table'

        relater.session.execute.return_value.scalar.return_value = 2480

        assert 2480 == relater._get_min_dst_event()
        relater.session.execute.assert_called_with("SELECT MAX(_last_dst_event) FROM rel_table")

    def test_get_next_max_src_event(self):
        relater = self._get_relater()
        relater.src_table_name = "src_table"
        relater.is_many = False
        relater._join_array_elements = lambda: 'JOIN ARR ELMS'
        start_eventid = 0
        max_rows = 400
        max_eventid = 200

        relater.session.execute.return_value.scalar.return_value = 300
        assert 300 == relater._get_next_max_src_event(start_eventid, max_rows, max_eventid)
        relater.session.execute.assert_called_with(
            "SELECT src._last_event FROM src_table src  "
            "WHERE src._last_event > 0 AND src._last_event <= 200 "
            "ORDER BY src._last_event OFFSET 400 - 1 LIMIT 1"
        )

        relater.is_many = True
        relater.session.execute.return_value.scalar.return_value = 300
        assert 300 == relater._get_next_max_src_event(start_eventid, max_rows, max_eventid)
        relater.session.execute.assert_called_with(
            "SELECT src._last_event FROM src_table src JOIN ARR ELMS "
            "WHERE src._last_event > 0 AND src._last_event <= 200 "
            "ORDER BY src._last_event "
            "OFFSET 400 - 1 "
            "LIMIT 1"
        )

        relater.session.execute.return_value.scalar.return_value = None
        assert 200 == relater._get_next_max_src_event(start_eventid, max_rows, max_eventid)

    def test_get_next_max_dst_event(self):
        relater = self._get_relater()
        relater.dst_table_name = "dst_table"
        start_eventid = 0
        max_rows = 400
        max_eventid = 200

        relater.session.execute.return_value.scalar.return_value = 300
        assert 300 == relater._get_next_max_dst_event(start_eventid, max_rows, max_eventid)
        relater.session.execute.assert_called_with(
            "SELECT _last_event FROM dst_table "
            "WHERE _last_event > 0 AND _last_event <= 200 "
            "ORDER BY _last_event "
            "OFFSET 400 - 1 "
            "LIMIT 1"
        )

        relater.session.execute.return_value.scalar.return_value = None
        assert 200 == relater._get_next_max_dst_event(start_eventid, max_rows, max_eventid)

    def test_get_chunks(self):
        relater = self._get_relater()

        relater._src_entities_range = MagicMock(side_effect=lambda min, max: f"src_{min}-{max}")
        relater._dst_entities_range = MagicMock(side_effect=lambda min=0, max=99: f"dst{min}-{max}")
        relater._get_next_max_src_event = MagicMock(side_effect=[5, 10, 15])
        relater._get_next_max_dst_event = MagicMock(side_effect=[20, 30, 40])

        res = list(relater._get_chunks(1, 14, 2, 38))

        expected = [
            ('src_1-5', 'dst0-99'),
            ('src_5-10', 'dst0-99'),
            ('src_10-15', 'dst0-99'),
            ('src_0-1', 'dst2-20'),
            ('src_0-1', 'dst20-30'),
            ('src_0-1', 'dst30-40'),
        ]
        self.assertEqual(expected, res)

        # Do again, but force only src side
        relater._get_next_max_src_event = MagicMock(side_effect=[5, 10, 15])
        relater._get_next_max_dst_event = MagicMock(side_effect=[20, 30, 40])
        res = list(relater._get_chunks(1, 14, 2, 51, True))

        expected = [
            ('src_1-5', 'dst0-99'),
            ('src_5-10', 'dst0-99'),
            ('src_10-15', 'dst0-99'),
        ]
        self.assertEqual(expected, res)

    def test_get_updates_chunked(self):
        relater = self._get_relater()
        relater.get_query = MagicMock()
        relater._query_into_results_table = MagicMock()
        relater._remove_duplicate_rows = MagicMock()

        relater._get_chunks = MagicMock(return_value=[
            ('src 1', 'dst 1'),
            ('src 2', 'dst 2'),
        ])
        start_src_event = 1
        max_src_event = 2
        start_dst_event = 3
        max_dst_event = 4

        # Default args
        relater._get_updates_chunked(start_src_event, max_src_event, start_dst_event, max_dst_event)
        relater._get_chunks.assert_called_with(start_src_event, max_src_event, start_dst_event, max_dst_event,
                                               only_src_side=False)
        relater.get_query.assert_has_calls([
            call('src 1', 'dst 1', max_src_event, max_dst_event, is_conflicts_query=False),
            call('src 2', 'dst 2', max_src_event, max_dst_event, is_conflicts_query=False),
        ])
        relater._query_into_results_table.assert_called_with(relater.get_query.return_value, False)
        relater._remove_duplicate_rows.assert_called_once()

        # With only_src_side and is_conflicts_query set
        relater.get_query.reset_mock()
        relater._remove_duplicate_rows.reset_mock()

        relater._get_updates_chunked(start_src_event, max_src_event, start_dst_event, max_dst_event,
                                     only_src_side=True, is_conflicts_query=True)
        relater._get_chunks.assert_called_with(start_src_event, max_src_event, start_dst_event, max_dst_event,
                                               only_src_side=True)
        relater.get_query.assert_has_calls([
            call('src 1', 'dst 1', max_src_event, max_dst_event, is_conflicts_query=True),
            call('src 2', 'dst 2', max_src_event, max_dst_event, is_conflicts_query=True),
        ])
        relater._query_into_results_table.assert_called_with(relater.get_query.return_value, True)
        relater._remove_duplicate_rows.assert_called_once()

    def test_remove_duplicate_rows(self):
        relater = self._get_relater()
        relater.dst_has_states = False

        relater._remove_duplicate_rows()
        relater.session.execute.assert_called_with(
            "DELETE FROM src_catalog_name_srcabbr_src_field_name_result WHERE rowid IN (  "
            "SELECT rowid   FROM ("
            "    SELECT rowid, row_number() OVER (PARTITION BY id ORDER BY dst_id) rn"
            "     FROM src_catalog_name_srcabbr_src_field_name_result  ) q WHERE rn > 1)"
        )

        relater.dst_has_states = True
        relater._remove_duplicate_rows()
        relater.session.execute.assert_called_with(
            "DELETE FROM src_catalog_name_srcabbr_src_field_name_result WHERE rowid IN (  "
            "SELECT rowid   FROM ("
            "    SELECT rowid, row_number() OVER (PARTITION BY id ORDER BY dst_id, dst_volgnummer DESC) rn"
            "     FROM src_catalog_name_srcabbr_src_field_name_result  ) q WHERE rn > 1)"
        )

    def test_query_into_results_table(self):
        relater = self._get_relater()
        relater.result_table_name = "result_table"
        relater._select_aliases = lambda is_conflicts: ["columnA", "columnB"] if is_conflicts else ["columnC",
                                                                                                    "columnD"]

        relater._query_into_results_table("the query", False)
        relater.session.execute.assert_called_with("INSERT INTO result_table (columnC, columnD) (the query)")

        relater._query_into_results_table("the query", True)
        relater.session.execute.assert_called_with("INSERT INTO result_table (columnA, columnB) (the query)")

    def test_get_updates(self):
        relater = self._get_relater()
        relater._get_changed_ranges = MagicMock(return_value=(1, 2, 3, 4))
        relater._get_updates_full = MagicMock()
        relater._get_updates_chunked = MagicMock()
        relater._create_delete_events_query = MagicMock()
        relater._query_into_results_table = MagicMock()
        relater._read_results = MagicMock(return_value=[{'a': 1}, {'b': 2}])

        # Update run
        with relater as rel_ctx:
            res = rel_ctx._get_updates()
        self.assertEqual(relater._read_results.return_value, list(res))

        relater._get_updates_chunked.assert_called_with(1, 2, 3, 4)
        relater._get_updates_full.assert_not_called()
        relater._query_into_results_table.assert_called_with(relater._create_delete_events_query.return_value)
        relater._create_delete_events_query.assert_called_with(1, 2, 3, 4)

        relater.session.execute.assert_called_with("ANALYZE src_catalog_name_srcabbr_src_field_name_result")

        # Initial load
        relater._get_updates_chunked.reset_mock()
        relater._get_updates_full.reset_mock()
        relater._query_into_results_table.reset_mock()
        relater._create_delete_events_query.reset_mock()

        with relater as rel_ctx:
            res = rel_ctx._get_updates(True)

        self.assertEqual(relater._read_results.return_value, list(res))

        relater._get_updates_full.assert_called_with(2, 4)
        relater._get_updates_chunked.assert_not_called()
        relater._query_into_results_table.assert_called_with(relater._create_delete_events_query.return_value)
        relater._create_delete_events_query.assert_called_with(1, 2, 3, 4)

        relater.session.execute.assert_called_with("ANALYZE src_catalog_name_srcabbr_src_field_name_result")

    def test_read_results(self):
        relater = self._get_relater()
        relater.result_table_name = "result_table"
        relater.session.stream_execute.return_value = [(("a", "b"), )]

        assert [{'a': 'b'}] == list(relater._read_results())
        relater.session.stream_execute.assert_called_with(
            "SELECT * FROM result_table", execution_options={"yield_per": 25_000}
        )

    def test_get_cahnged_ranges(self):
        relater = self._get_relater()
        relater._get_min_src_event = MagicMock()
        relater._get_min_dst_event = MagicMock()
        relater._get_max_src_event = MagicMock()
        relater._get_max_dst_event = MagicMock()

        self.assertEqual((
            relater._get_min_src_event.return_value,
            relater._get_max_src_event.return_value,
            relater._get_min_dst_event.return_value,
            relater._get_max_dst_event.return_value,
        ), relater._get_changed_ranges())

    def test_get_updates_full(self):
        relater = self._get_relater()
        relater._get_updates_chunked = MagicMock()
        relater._get_updates_full(100, 200)
        relater._get_updates_chunked.assert_called_with(0, 100, 0, 200, only_src_side=True, is_conflicts_query=False)

        relater._get_updates_full(100, 200, True)
        relater._get_updates_chunked.assert_called_with(0, 100, 0, 200, only_src_side=True, is_conflicts_query=True)

    def test_get_conflicts(self):
        relater = self._get_relater()
        relater._get_max_src_event = MagicMock(return_value=50)
        relater._get_max_dst_event = MagicMock(return_value=60)
        relater._get_updates_full = MagicMock()
        relater._read_results = MagicMock(return_value=[{"a": "b"}])

        with relater:
            pass

        assert [{"a": "b"}] == list(relater.get_conflicts())
        relater._get_updates_full.assert_called_with(50, 60, is_conflicts_query=True)

    @patch("gobupload.relate.update._RELATE_VERSION", "123.0")
    @patch("gobupload.relate.update.EventCreator")
    @patch("gobupload.relate.update.EventCollector")
    @patch("gobupload.relate.update.ContentsWriter")
    @patch("gobupload.relate.update.ProgressTicker", MagicMock())
    def test_update(self, mock_contents_writer, mock_event_collector, mock_event_creator):
        relater = self._get_relater()
        relater._is_initial_load = MagicMock()
        relater._get_updates = MagicMock(return_value=iter([{'a': 1}, {'b': 2}, {'c': 3}]))
        relater._format_relation = MagicMock(side_effect=lambda x: x)
        relater._check_preconditions = MagicMock()

        mock_event_creator.return_value.create_event.side_effect = lambda x: x

        result = relater.update()

        mock_event_collector.assert_called_with(
            mock_contents_writer.return_value.__enter__.return_value,
            confirms_writer=None,
            version="123.0"
        )
        mock_event_collector.return_value.__enter__.return_value.collect.assert_has_calls([
            call({'a': 1}),
            call({'b': 2}),
            call({'c': 3}),
        ])
        assert mock_contents_writer.return_value.__enter__.return_value.filename == result

        # test dont add empty events
        mock_event_collector.reset_mock()
        mock_event_creator.return_value.create_event.side_effect = lambda x: None
        result = relater.update()
        mock_event_collector.collect.assert_not_called()

    def test_update_failing_precondition(self):
        relater = self._get_relater()
        relater._get_query = MagicMock()
        relater._check_preconditions = MagicMock(side_effect=Exception)

        with self.assertRaises(Exception):
            relater.update()

            # Make sure nothing is done yet. Check for preconditions is done first.
            relater._get_query.assert_not_called()
