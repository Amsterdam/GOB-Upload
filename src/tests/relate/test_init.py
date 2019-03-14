from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from gobupload.relate import build_relations, publish_relations

@patch('gobupload.relate.logger', MagicMock())
class TestInit(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('gobupload.relate.publish')
    def test_publish_empty_relations(self, mocked_publish):
        msg = {
            "header": {}
        }
        publish_relations(msg, [], False, False)
        mocked_publish.assert_called()

    @patch('gobupload.relate.publish')
    def test_publish_relations(self, mocked_publish):
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
                'num_records': 1
            },
            'contents': [{
                             'source': 'src_source.dst_source',
                             'id': 'src_id.src_volgnummer.dst_id.dst_volgnummer',
                             'src_source': 'src_source',
                             'src_id': 'src_id',
                             'src_volgnummer': 'src_volgnummer',
                             'derivation': 'key',
                             'begin_geldigheid': 'begin',
                             'eind_geldigheid': 'eind',
                             'dst_source': 'dst_source',
                             'dst_id': 'dst_id',
                             'dst_volgnummer': 'dst_volgnummer',
                             '_source_id': 'src_id.src_volgnummer.dst_id.dst_volgnummer'
                         }]
        }
        publish_relations(msg, relations.copy(), False, False)
        mocked_publish.assert_called_with('gob.workflow.proposal', 'fullimport.proposal', expect)

        expect["contents"][0]["id"] = expect["contents"][0]["id"] + ".begin"
        expect["contents"][0]["_source_id"] = expect["contents"][0]["id"]
        publish_relations(msg, relations.copy(), False, True)
        mocked_publish.assert_called_with('gob.workflow.proposal', 'fullimport.proposal', expect)

    @patch('gobupload.relate.apply_relations', MagicMock())
    @patch('gobupload.relate.GOBModel', MagicMock())
    def test_build_relations(self):
        with self.assertRaises(AssertionError):
            build_relations({})

        build_relations({"catalogue": "catalog"})
