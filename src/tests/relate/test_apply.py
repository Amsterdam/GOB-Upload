from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from gobcore.model import GOBModel

from gobupload.relate.apply import update_row_relation, clear_row_relation, get_next_item, get_match, _get_field_type, match_relation, apply_relations, prepare_row
from gobupload.storage.relate import RelationUpdater

class TestApply(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_next_item(self):
        items = iter([])
        self.assertIsNone(get_next_item(items))

        items = iter([1, 2])
        self.assertEqual(get_next_item(items), 1)
        self.assertEqual(get_next_item(items), 2)
        self.assertIsNone(get_next_item(items))

    def test_update_row_relation_single_reference(self):
        row = {
            'field': {'bronwaarde': "bronwaarde"}
        }
        relation = {
            'dst': [{
                'id': 'id'
            }]
        }
        field_name = "field"
        field_type = "GOB.Reference"
        result = update_row_relation(row, relation, field_name, field_type)
        self.assertEqual(result, True)
        self.assertEqual(row['field'], {
            '_id': 'id',
            'bronwaarde': "bronwaarde"
        })

    def test_update_row_relation_multi_reference(self):
        row = {
            'field': [{'bronwaarde': "bronwaarde1"}, {'bronwaarde': "bronwaarde2"}]
        }
        relation = {
            'dst': [{
                'id': 'id1',
                'bronwaardes': ['bronwaarde1']
            }, {
                    'id': 'id2',
                    'bronwaardes': ['bronwaarde2']
            }]
        }
        field_name = "field"
        field_type = "GOB.ManyReference"
        result = update_row_relation(row, relation, field_name, field_type)
        self.assertEqual(result, True)
        self.assertEqual(row['field'], [
            {'bronwaarde': 'bronwaarde1', '_id': 'id1'},
            {'bronwaarde': 'bronwaarde2', '_id': 'id2'}
        ])

        result = update_row_relation(row, relation, field_name, field_type)
        self.assertEqual(result, False)

        relation['dst'].pop(0)
        result = update_row_relation(row, relation, field_name, field_type)
        self.assertEqual(result, True)
        self.assertEqual(row['field'], [
            {'bronwaarde': 'bronwaarde1', '_id': None},
            {'bronwaarde': 'bronwaarde2', '_id': 'id2'}
        ])

        result = update_row_relation(row, relation, field_name, field_type)
        self.assertEqual(result, False)

    def test_clear_row_relation_single_reference(self):
        row = {
            'field': {'bronwaarde': "bronwaarde"}
        }
        field_name = "field"
        field_type = "GOB.Reference"
        result = clear_row_relation(row, field_name, field_type)
        self.assertEqual(result, True)
        self.assertEqual(row['field'], {
            '_id': None,
            'bronwaarde': "bronwaarde"
        })

        result = clear_row_relation(row, field_name, field_type)
        self.assertEqual(result, False)

    def test_clear_row_relation_multi_reference(self):
        row = {
            'field': [{'bronwaarde': "bronwaarde1"}, {'bronwaarde': "bronwaarde2"}]
        }
        field_name = "field"
        field_type = "GOB.ManyReference"
        result = clear_row_relation(row, field_name, field_type)
        self.assertEqual(result, True)
        self.assertEqual(row['field'], [
            {'bronwaarde': 'bronwaarde1', '_id': None},
            {'bronwaarde': 'bronwaarde2', '_id': None}
        ])

        result = clear_row_relation(row, field_name, field_type)
        self.assertEqual(result, False)

    def test_match(self):
        current_relation = None
        relation = None
        result = get_match(relation, current_relation)
        self.assertEqual(result, (False, False, False))

        relation = {}
        result = get_match(relation, current_relation)
        self.assertEqual(result, (False, False, False))

        current_relation = {'src': {'source': 'source', 'id': 'id', 'volgnummer': None}, 'eind_geldigheid': None}
        relation = {'_source': 'source', '_id': 'id'}
        result = get_match(relation, current_relation)
        self.assertEqual(result, (True, True, False))

        current_relation = {'src': {'source': 'source', 'id': 'id', 'volgnummer': 1}, 'eind_geldigheid': None}
        relation = {'_source': 'source', '_id': 'id', 'volgnummer': 1, 'eind_geldigheid': None}
        result = get_match(relation, current_relation)
        self.assertEqual(result, (True, True, False))

        current_relation = {'src': {'source': 'source', 'id': 'id', 'volgnummer': 1}, 'eind_geldigheid': None}
        relation = {'_source': 'source', '_id': 'id', 'volgnummer': 2, 'eind_geldigheid': None}
        result = get_match(relation, current_relation)
        self.assertEqual(result, (True, False, False))

        current_relation = {'src': {'source': 'source', 'id': 'id', 'volgnummer': 1}, 'eind_geldigheid': 'eind'}
        relation = {'_source': 'source', '_id': 'id', 'volgnummer': 1, 'eind_geldigheid': None}
        result = get_match(relation, current_relation)
        self.assertEqual(result, (True, False, False))


    def test_field_type(self):
        mock_collection = {
            'all_fields': {
                'field': {
                    'type': 'GOB.Reference'
                }
            }
        }
        with patch.object(GOBModel, 'get_collection', lambda *args: mock_collection):
            result = _get_field_type("catalog", "collection", "field")
            self.assertEqual(result, 'GOB.Reference')

        mock_collection['all_fields']['field']['type'] = 'GOB.ManyReference'
        with patch.object(GOBModel, 'get_collection', lambda *args: mock_collection):
            result = _get_field_type("catalog", "collection", "field")
            self.assertEqual(result, 'GOB.ManyReference')

        mock_collection['all_fields']['field']['type'] = 'Any other type'
        with self.assertRaises(AssertionError):
            with patch.object(GOBModel, 'get_collection', lambda *args: mock_collection):
                result = _get_field_type("catalog", "collection", "field")

    @patch('gobupload.relate.apply.logger', MagicMock())
    @patch('gobupload.relate.apply.get_match')
    @patch('gobupload.relate.apply.update_row_relation')
    @patch('gobupload.relate.apply.clear_row_relation')
    def test_match_relation(self, mock_clear_row, mock_update_row, mock_get_match):
        mock_clear_row.return_value = "clear row"
        mock_update_row.return_value = "update row"
        mock_get_match.return_value = (True, True, False)
        relation = "relation"
        field_name = "field"
        field_type = "type"
        current_relation = {
            "field": None,
            "_id": "any id",
            field_name: {
                "bronwaarde": "any bronwaarde"
            }
        }
        result = match_relation(current_relation, relation, field_name, field_type)
        self.assertEqual(result, ("update row", True, True))

        mock_get_match.return_value = (False, False, False)
        result = match_relation(current_relation, relation, field_name, field_type)
        self.assertEqual(result, ("clear row", True, True))

        mock_get_match.return_value = (False, True, False)
        result = match_relation(current_relation, relation, field_name, field_type)
        self.assertEqual(result, ("clear row", True, True))

        mock_get_match.return_value = (True, False, False)
        result = match_relation(current_relation, relation, field_name, field_type)
        self.assertEqual(result, (False, False, True))

        mock_get_match.return_value = (True, False, True)
        result = match_relation(current_relation, relation, field_name, field_type)
        self.assertEqual(result, (False, True, False))

        mock_get_match.return_value = (False, False, False)
        result = match_relation(None, None, field_name, field_type)
        self.assertEqual(result, (False, False, False))


    @patch('gobupload.relate.apply.match_relation')
    @patch('gobupload.relate.apply._get_field_type')
    @patch('gobupload.relate.apply.get_current_relations')
    def test_apply_relations(self, mock_current_relations, mock_field_type, mock_match_relation):
        mock_current_relations.return_value = iter([])
        mock_field_type.return_value = ''
        mock_match_relation.return_value = (True, False, False)
        with patch.object(RelationUpdater, 'update'):
            result = [r for r in apply_relations("catalog", "collection", "field", iter([]))]
            self.assertEqual(result, [])

    def test_prepare_row(self):
        row = {"field": "value"}
        prepare_row(row, "field", "any type")
        self.assertEqual(row, {"field": "value"})

        row["field"] = None
        prepare_row(row, "field", "GOB.Reference")
        self.assertEqual(row, {"field": {}})

        row["field"] = None
        prepare_row(row, "field", "GOB.ManyReference")
        self.assertEqual(row, {"field": []})
