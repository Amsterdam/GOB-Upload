import importlib
import unittest
from unittest.mock import MagicMock, patch

from sqlalchemy import MetaData
from sqlalchemy.engine import Connection

from gobcore.exceptions import GOBException

from gobupload.storage.handler import with_session, StreamSession
from gobupload.storage import handler
from tests import fixtures


class MockMeta:
    source = "AMSBI"
    catalogue = "meetbouten"
    entity = "meetbouten"


class TestWithSession(unittest.TestCase):
    session = None
    in_method = None

    @with_session
    def annotated_method(self, **kwargs):
        self.in_method = kwargs

    def test_annotated_method(self):
        self.session = None

        # assert annotated method raises Exception when called without session in class
        with self.assertRaises(GOBException):
            self.annotated_method({})

        # assert annotated method runs when session is not None
        self.session = fixtures.random_string()
        params = fixtures.random_dict()
        self.assertIsNone(self.in_method)
        self.annotated_method(**params)
        self.assertEqual(params, self.in_method)


class TestContextManager(unittest.TestCase):

    def setUp(self):
        # patch __init__, we don't test that here, but we need session and engine to be present
        def side_effect(self, param=None):
            self.metadata = param
            self.session = None
            self.engine = MagicMock()

            if param:
                self.tablename_temp = handler.GOBStorageHandler._generate_temp_tablename(param)
        handler.GOBStorageHandler.__init__ = side_effect

    def tearDown(self):
        # restore in setUp patched code
        importlib.reload(handler)

    def test_session_context(self):
        mock_session = MagicMock()
        handler.GOBStorageHandler.Session = mock_session
        storage = handler.GOBStorageHandler(MockMeta())

        # assert starting situation
        self.assertIsNone(storage.session)

        # prepare mock
        mock_session_instance = MagicMock()
        mock_session.return_value = mock_session_instance

        mock_conn = MagicMock()
        storage.engine.connect.return_value = mock_conn

        # test session creation in context
        with storage.get_session() as session:
            mock_session.assert_called_with(bind=mock_conn)
            self.assertEqual(storage.session, mock_session_instance)
            self.assertEqual(session, mock_session_instance)

        # test session creation after leaving context:
        mock_session_instance.commit.assert_called()
        mock_session_instance.close.assert_called()
        self.assertIsNone(storage.session)

        # test exception handling
        with self.assertRaises(ConnectionError):
            with patch("gobupload.storage.handler.logger") as mock_logger:
                with storage.get_session():
                    raise ConnectionError("any")

        mock_session_instance.rollback.assert_called()
        mock_session_instance.close.assert_called()
        mock_logger.error.assert_called_with("ConnectionError('any')")

    def test_session_context_invalidate(self):
        mock_session = MagicMock(spec=StreamSession)
        handler.GOBStorageHandler.Session = mock_session

        with patch("gobupload.storage.handler.random_string", return_value="abcdefgh"):
            storage = handler.GOBStorageHandler(MockMeta())

        mock_conn = MagicMock(spec=Connection)
        storage.engine.connect.return_value = mock_conn

        mock_session_instance = MagicMock(spec=StreamSession)
        mock_session.return_value = mock_session_instance

        with patch.object(storage.base, "metadata", spec=MetaData) as mock_meta:
            mock_meta.tables = {}

            with storage.get_session(invalidate=True):
                pass

            mock_conn.invalidate.assert_called()
            mock_session_instance.close.assert_called()
            mock_meta.remove.assert_not_called()

            mock_conn.invalidate.reset_mock()
            mock_session_instance.close.reset_mock()
            mock_meta.tables = {"tmp_meetbouten_meetbouten_abcdefgh": "my_table_obj"}

            with storage.get_session(invalidate=True):
                pass

            mock_conn.invalidate.assert_called()
            mock_session_instance.close.assert_called()
            mock_meta.remove.assert_called_with("my_table_obj")

    @patch("gobupload.storage.handler.random_string", MagicMock(return_value="abcdefgh"))
    def test_session_context_invalidate_no_metadata(self):
        storage = handler.GOBStorageHandler()

        mock_conn = MagicMock(spec=Connection)
        storage.engine.connect.return_value = mock_conn

        with patch.object(storage.base, "metadata", spec=MetaData) as mock_meta:
            with storage.get_session(invalidate=True):
                pass

        mock_conn.invalidate.assert_called()
        mock_meta.remove.assert_not_called()
