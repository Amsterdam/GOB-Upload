import importlib
import unittest
from unittest.mock import call, MagicMock, patch

from gobcore.events.import_message import ImportMessage
from gobcore.model import GOBModel

from gobupload.compare import populate
from gobupload.storage import queries
from gobupload.storage.handler import GOBStorageHandler
from tests import fixtures


class TestStorageHandler(unittest.TestCase):

    @patch('gobupload.storage.handler.create_engine', MagicMock())
    def setUp(self):
        self.mock_model = MagicMock(spec=GOBModel)
        self.msg = fixtures.get_message_fixture()
        model = {
            "entity_id": "identificatie",
            "version": "1"
        }
        # Add the hash to the message
        populate(self.msg, model)

        message = ImportMessage(self.msg)
        metadata = message.metadata

        self.storage = GOBStorageHandler(metadata)

    def test_create_temporary_table(self):
        expected_table = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}_tmp'

        self.storage.create_temporary_table(self.msg["contents"])

        # Assert the test table has been made
        self.assertIn(expected_table, self.storage.base.metadata.tables)

        # And the engine has been called to fill the temporary table
        self.storage.engine.execute.assert_called()

    def test_create_temporary_table_exists(self):
        expected_table = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}_tmp'

        mock_table = MagicMock()

        # Make sure the test table already exists
        self.storage.base.metadata.tables = {expected_table: mock_table}
        self.storage.create_temporary_table(self.msg["contents"])

        # Assert the truncate function is called
        self.storage.engine.execute.assert_any_call(f"TRUNCATE {expected_table}")

        # And the engine has been called to fill the temporary table
        self.storage.engine.execute.assert_called()

    def test_compare_temporary_data(self, mock_get_comparison):
        current_table = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}'
        new_table = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}_tmp'

        self.storage.compare_temporary_data()

        # Check if the get comparison function is called for confirms and changes
        mock_get_comparison.assert_any_call(current_table, new_table)
        mock_get_comparison.assert_any_call(current_table, new_table, False)

        # Assert the temporary table is deleted
        self.storage.engine.execute.assert_any_call(f"DROP TABLE {new_table}")

    def test_compare_temporary_data(self):
        current = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}'
        temporary = f'{self.msg["header"]["catalogue"]}_{self.msg["header"]["entity"]}_tmp'

        fields = ['_source', 'identificatie']
        query = queries.get_comparison_query(current, temporary, fields)

        self.storage.compare_temporary_data()

        # Assert the query is performed is deleted
        self.storage.engine.execute.assert_any_call(query)

        # Assert the temporary table is deleted
        self.storage.engine.execute.assert_any_call(f"DROP TABLE IF EXISTS {temporary}")
