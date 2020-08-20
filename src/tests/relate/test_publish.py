from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from gobupload.relate.publish import publish_result

@patch('gobupload.relate.publish.logger', MagicMock())
class TestInit(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_publish_result(self):
        msg = {
            'header': 'any header',
            'anything else': 'any values'
        }
        relates = "any relates"
        result = publish_result(msg, relates)
        self.assertEqual(result, {
            'header': msg['header'],
            'summary': mock.ANY,
            'contents': relates
        })
