from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobupload.utils import ActiveGarbageCollection


class TestUpdate(TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('gobupload.utils.gc')
    def testGarbageCollection(self, mocked_gc):
        mocked_gc.collect = MagicMock(return_value=1)
        with ActiveGarbageCollection("any title") as agc:
            self.assertEqual(agc.title, "any title")
            self.assertEqual(mocked_gc.collect.call_count, 1)
        self.assertEqual(mocked_gc.collect.call_count, 2)
