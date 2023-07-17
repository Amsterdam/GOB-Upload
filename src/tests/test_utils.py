from unittest import TestCase

from string import ascii_lowercase

from gobupload.utils import random_string


class TestUpdate(TestCase):

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
