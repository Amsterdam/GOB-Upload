from unittest import TestCase
from unittest.mock import patch

from gobupload.dev_utils.relate_interval_table import run


class TestRelateIntervalTable(TestCase):

    @patch("gobupload.dev_utils.relate_interval_table.StartValiditiesTable")
    @patch("gobupload.dev_utils.relate_interval_table.sys")
    def test_run(self, mock_sys, mock_table):

        mock_sys.argv = []

        with self.assertRaises(AssertionError):
            run()

        mock_sys.argv = ['relate_interval_table.py', 'catalog', 'collection', 'table_name']
        run()
        mock_table.from_catalog_collection.assert_called_with('catalog', 'collection', 'table_name')
        mock_table.from_catalog_collection().create.assert_called_once()
