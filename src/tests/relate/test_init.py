from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from gobupload.relate import build_relations, _relation_needs_update

@patch('gobupload.relate.logger', MagicMock())
class TestInit(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('gobupload.relate.apply_relations', MagicMock())
    @patch('gobupload.relate.GOBModel', MagicMock())
    def test_build_relations(self):
        with self.assertRaises(AssertionError):
            build_relations({})

    def test_needs_update(self):
        result = _relation_needs_update("catalog", "collection", "reference", {"ref": "dst_cat:dst_col"})
        self.assertEqual(result, False)
