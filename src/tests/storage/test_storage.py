import importlib
import unittest
from unittest import mock

from gobcore.exceptions import GOBException

from gobupload.storage.handler import with_session
from gobupload.storage import handler
from tests import fixtures


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
        def side_effect(self, param):
            self.session = None
            self.engine = None
        handler.GOBStorageHandler.__init__ = side_effect

    def tearDown(self):
        # restore in setUp patched code
        importlib.reload(handler)

    @mock.patch("gobupload.storage.handler.Session")
    def test_session_context(self, mock_session):
        storage = handler.GOBStorageHandler(fixtures.random_string())

        # assert starting situation
        self.assertIsNone(storage.session)

        # prepare mock
        mock_session_instance = mock.MagicMock()
        mock_session.return_value = mock_session_instance

        # test session creation in context
        with storage.get_session():
            mock_session.assert_called_with(storage.engine)
            self.assertEqual(storage.session, mock_session_instance)

        # test session creation after leaving context:
        mock_session_instance.commit.assert_called()
        mock_session_instance.close.assert_called()
        self.assertIsNone(storage.session)

