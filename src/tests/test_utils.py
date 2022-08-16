from string import ascii_lowercase
from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobupload.utils import ActiveGarbageCollection, random_string


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

    def test_random_string(self):
        self.assertEqual(6, len(random_string(6)))
        self.assertEqual(8, len(random_string(8)))

        expected_characters = ascii_lowercase + '0123456789'

        self.assertTrue(all([c in expected_characters for c in random_string(200)]))
        self.assertTrue(all([c in expected_characters for c in random_string(10)]))

        try:
            self.assertNotEqual(random_string(1000), random_string(1000))
        except AssertionError:
            # Just in the very very very small case the first two strings are equal
            self.assertNotEqual(random_string(1000), random_string(1000))
