from unittest import TestCase
from unittest.mock import patch

from gobupload.dev_utils.relate_query import run


class TestRelateQuery(TestCase):

    @patch("builtins.print")
    @patch("gobupload.dev_utils.relate_query.RelationTableRelater")
    @patch("gobupload.dev_utils.relate_query.sys")
    def test_run(self, mock_sys, mock_relater, mock_print):

        mock_sys.argv = []
        with self.assertRaises(AssertionError):
            run()

        mock_sys.argv = ['relate_query.py', 'catalog', 'collection', 'attribute']
        run()
        mock_relater.assert_called_with('catalog', 'collection', 'attribute')
        mock_relater.return_value.get_query.assert_called_with(False)
        mock_print.assert_called_with(mock_relater.return_value.get_query.return_value)

        mock_sys.argv = ['relate_query.py', 'catalog', 'collection', 'attribute', 'true']
        run()
        mock_relater.assert_called_with('catalog', 'collection', 'attribute')
        mock_relater.return_value.get_query.assert_called_with(True)
        mock_print.assert_called_with(mock_relater.return_value.get_query.return_value)
