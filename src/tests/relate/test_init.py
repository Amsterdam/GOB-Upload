from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from gobupload.relate import build_relations, check_relation, _relation_needs_update, _process_references, _log_exception

mock_logger = MagicMock()
@patch('gobupload.relate.logger', mock_logger)
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
            'summary': {'warnings': mock.ANY, 'errors': []},
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

    @patch('gobupload.relate.GOBModel', MagicMock())
    @patch('gobupload.relate.logger', MagicMock())
    @patch('gobupload.relate.check_relations', MagicMock())
    def test_check_relation(self):
        msg = {
            'header': {
                'catalog': 'any catalog',
                'collections': 'any collections'
            }
        }
        result = check_relation(msg)
        self.assertEqual(result, {
            'header': msg['header'],
            'summary': mock.ANY,
            'contents': None
        })

    def test_log_exception(self):
        mock_logger.error = MagicMock()
        mock_logger.error.assert_not_called()
        _log_exception("any msg", "any err")
        mock_logger.error.assert_called_with("any msg: any err")
        _log_exception("any msg", "any err", 5)
        mock_logger.error.assert_called_with("any m...")
