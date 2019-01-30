from unittest import TestCase

from gobupload import get_report

class TestInit(TestCase):

    def test_report(self):
        report = get_report(
            contents=[1, 2, 3],
            events=[{"event": "ADD"}],
            recompares=[4])
        self.assertEqual(report['num_records'], 1)
        self.assertEqual(report['num_skipped_historical'], 1)
