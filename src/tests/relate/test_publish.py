from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from gobcore.message_broker.config import RESULT_QUEUE

from gobupload.relate.publish import publish_relations, publish_result

@patch('gobupload.relate.publish.logger', MagicMock())
class TestInit(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_publish_empty_relations(self):
        msg = {
            "header": {}
        }
        result = publish_relations(msg, [], False, False)
        self.assertEqual(result, {
            "header": msg["header"],
            'summary': {
                'num_records': 0,
                'warnings': mock.ANY,
                'errors': mock.ANY
            },
            'contents_ref': mock.ANY
        })

    def test_publish_relations(self):
        relations = [
            {
                "src": {
                    "source": "src_source",
                    "id": "src_id",
                    "volgnummer": "src_volgnummer"
                },
                "begin_geldigheid": "begin",
                "eind_geldigheid": "eind",
                "dst": [{
                    "source": "dst_source",
                    "id": "dst_id",
                    "volgnummer": "dst_volgnummer"
                }]
            }
        ]
        msg = {
            "header": {}
        }
        expect = {
            'header': {},
            'summary': {
                'num_records': 1,
                'warnings': mock.ANY,
                'errors': mock.ANY
            },
            'contents_ref': mock.ANY
            # 'contents': [{
            #     'source': 'src_source.dst_source',
            #     'id': 'src_id.src_volgnummer.dst_id.dst_volgnummer',
            #     'src_source': 'src_source',
            #     'src_id': 'src_id',
            #     'src_volgnummer': 'src_volgnummer',
            #     'derivation': 'key',
            #     'begin_geldigheid': 'begin',
            #     'eind_geldigheid': 'eind',
            #     'dst_source': 'dst_source',
            #     'dst_id': 'dst_id',
            #     'dst_volgnummer': 'dst_volgnummer',
            #     '_source_id': 'src_id.src_volgnummer.dst_id.dst_volgnummer'
            # }]
        }
        result = publish_relations(msg, relations.copy(), False, False)
        self.assertEqual(result, expect)

        # expect["contents"][0]["id"] = expect["contents"][0]["id"] + ".begin"
        # expect["contents"][0]["_source_id"] = expect["contents"][0]["id"]

        result = publish_relations(msg, relations.copy(), False, True)
        self.assertEqual(result, expect)

    def test_publish_result(self):
        msg = {
            'header': 'any header',
            'anything else': 'any values'
        }
        relates = "any relates"
        result = publish_result(msg, relates)
        self.assertEqual(result, {
            'header': msg['header'],
            'summary': {
                'warnings': mock.ANY,
                'errors': mock.ANY
            },
            'contents': relates
        })
