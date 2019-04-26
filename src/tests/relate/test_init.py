from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from gobupload.relate import build_relations, _relation_needs_update, _process_references

@patch('gobupload.relate.logger', MagicMock())
class TestInit(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_build_relations(self):
        with self.assertRaises(AssertionError):
            build_relations({})

    @patch('gobupload.relate.GOBModel', MagicMock())
    def test_build_relations(self):
        result = build_relations({'catalogue': 'any catalogue'})
        self.assertEqual(result, {
            'header': mock.ANY,
            'summary': {'warnings': [], 'errors': []},
            'contents': []
        })

    def test_needs_update(self):
        result = _relation_needs_update("catalog", "collection", "reference", {"ref": "dst_cat:dst_col"})
        self.assertEqual(result, False)

    @patch('gobupload.relate._relation_needs_update')
    def test_has_sources(self, mock_needs_update):
        mock_needs_update.return_value = True
        result = _process_references({}, "catalog", "collection", {})
        self.assertEqual(result, [])
