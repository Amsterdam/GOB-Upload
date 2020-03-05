from unittest import TestCase
from unittest.mock import MagicMock

from gobupload.update.update_statistics import UpdateStatistics


class TestUpdateStatistics(TestCase):

    def test_get_applied_stats(self):
        us = UpdateStatistics()
        us._get_stats = MagicMock()
        us.applied = MagicMock()

        self.assertEqual(us._get_stats.return_value, us.get_applied_stats())
        us._get_stats.assert_called_with(us.applied)

    def test_get_stats(self):
        stats = {
            'ADD': 25,
            'CONFIRM': 50,
            'SOMETHING_ELSE': 25,
        }
        expected = {
            'ADD': {
                'absolute': 25,
                'relative': 0.25,
            },
            'CONFIRM': {
                'absolute': 50,
                'relative': 0.5,
            },
            'SOMETHING_ELSE': {
                'absolute': 25,
                'relative': 0.25,
            }
        }
        us = UpdateStatistics()

        self.assertEqual(expected, us._get_stats(stats))
