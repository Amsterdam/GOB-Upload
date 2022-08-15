import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from string import ascii_lowercase

from gobcore.utils import get_filename

from gobupload.utils import ActiveGarbageCollection, random_string, \
    load_offloaded_message_data


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

    def test_load_offloaded_message_data(self):
        msg = {
            "contents_ref": "offloaded.json",
        }
        with TemporaryDirectory() as tmpdir:
            with mock.patch("gobcore.utils.GOB_SHARED_DIR", str(tmpdir)):
                fname = get_filename(msg["contents_ref"], "message_broker")
                with Path(fname).open("w") as fp:
                    json.dump([{"offloaded": "data"}], fp)

                data = load_offloaded_message_data(msg)
            assert list(data["contents"]) == [{"offloaded": "data"}]
