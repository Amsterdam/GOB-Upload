from unittest import TestCase
from unittest.mock import patch, call

from gobupload.storage.execute import _execute, _execute_multiple


class TestExecute(TestCase):

    @patch("gobupload.storage.execute.GOBStorageHandler")
    def test_execute_multiple(self, mock_handler):
        res = _execute_multiple(['query1', 'query2'])

        handler_instance = mock_handler.return_value
        handler_instance.get_session.assert_called_once()

        session_instance = handler_instance.get_session.return_value.__enter__.return_value
        session_instance.connection.assert_called_with(execution_options={'stream_results': True})

        connection_instance = session_instance.connection.return_value
        connection_instance.execute.assert_has_calls([call('query1'), call('query2')])

        self.assertEqual(connection_instance.execute.return_value, res)

    @patch("gobupload.storage.execute._execute_multiple")
    def test_execute(self, mock_execute_multiple):
        res = _execute('some query')

        mock_execute_multiple.assert_called_with(['some query'])
        self.assertEqual(mock_execute_multiple.return_value, res)
