from unittest import TestCase, mock
from unittest.mock import MagicMock, patch, call

from gobcore.exceptions import GOBException
from gobupload.relate import build_relations, check_relation, _relation_needs_update, _process_references, \
    _log_exception, _split_job, update_materialized_view, _get_materialized_view_by_relation_name, \
    _get_materialized_view, WORKFLOW_EXCHANGE, WORKFLOW_REQUEST_KEY, _relation_needs_update

mock_logger = MagicMock()
@patch('gobupload.relate.logger', mock_logger)
class TestInit(TestCase):

    class MockModel:
        collections = {
            'collA': {
                'attributes': ['attrA', 'attrB', 'attrC']
            },
            'colB': {
                'attributes': [],
            },
            'dst_col': {
                'attributes': ['attr'],
            }
        }

        def get_catalog(self, catalog):
            return {'catalog': ''}

        def get_collection_names(self, catalog):
            return self.collections.keys()

        def get_collection(self, catalog, collection):
            return self.collections.get(collection)

        def _extract_references(self, attributes):
            return attributes

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_needs_update(self):
        result = _relation_needs_update("catalog", "collection", "reference", {"ref": "dst_cat:dst_col"})
        self.assertEqual(result, False)

    @patch("gobupload.relate.get_last_change")
    @patch("gobupload.relate.get_relation_name", MagicMock())
    @patch("gobupload.relate.GOBModel")
    def test_relation_needs_update(self, mock_model, mock_get_last_change):
        mock_model.return_value = self.MockModel()
        mock_get_last_change.side_effect = [1,2,3]
        
        # No update needed if relations are further than src or dest
        self.assertEqual(False, _relation_needs_update('catalog', 'collA', 'ref', {'ref': 'dst_cat:dst_col'}))

        # Update needed if relations are behind src or dest
        mock_get_last_change.side_effect = [3,2,1]
        self.assertEqual(True, _relation_needs_update('catalog', 'collA', 'ref', {'ref': 'dst_cat:dst_col'}))

        # Return false if dst not found
        self.assertEqual(False, _relation_needs_update('catalog', 'collA', 'ref', {'ref': 'dst_cat:dst_colB'}))


    @patch('gobupload.relate._relation_needs_update')
    def test_has_sources(self, mock_needs_update):
        mock_needs_update.return_value = True
        result = _process_references({}, "catalog", "collection", {})
        self.assertEqual(result, [])

    @patch('gobupload.relate._log_exception')
    @patch('gobupload.relate.GOBModel', MagicMock())
    @patch('gobupload.relate.logger', MagicMock())
    @patch('gobupload.relate.check_relations')
    @patch('gobupload.relate.check_relation_conflicts', MagicMock())
    def test_check_relation(self, mock_check_relations, _log_exception):
        msg = {
            'header': {
                'catalogue': 'any catalog',
                'collections': 'any collections'
            }
        }
        result = check_relation(msg)
        self.assertEqual(result, {
            'header': msg['header'],
            'summary': mock.ANY,
            'contents': None
        })
        
        mock_check_relations.side_effect = Exception
        result = check_relation(msg)
        
        _log_exception.assert_called()

    def test_log_exception(self):
        mock_logger.error = MagicMock()
        mock_logger.error.assert_not_called()
        _log_exception("any msg", "any err")
        mock_logger.error.assert_called_with("any msg: any err")
        _log_exception("any msg", "any err", 5)
        mock_logger.error.assert_called_with("any m...")

    def _get_split_msg(self, original_msg, catalog, collection, attribute):
        header = {k: v for k, v in original_msg['header'].items()}

        del header['jobid']
        del header['stepid']
        return {
            **original_msg,
            'header': {
                **header,
                'catalogue': catalog,
                'collection': collection,
                'attribute': attribute,
                'split_from': original_msg['header']['jobid'],
            },
            'workflow': {
                'workflow_name': 'relate',
            }
        }

    def assertSplitJobsPublished(self, connection, messages):
        connection_instance = connection.return_value.__enter__.return_value
        calls = [call(WORKFLOW_EXCHANGE, WORKFLOW_REQUEST_KEY, message) for message in messages]
        connection_instance.publish.assert_has_calls(calls)
        self.assertEqual(len(messages), connection_instance.publish.call_count)

    @patch("gobupload.relate.GOBModel", MockModel)
    @patch("gobupload.relate.GOBSources")
    @patch("gobupload.relate.MessageBrokerConnection")
    def test_split_job(self, mock_connection, mock_sources):
        mock_sources.return_value.get_field_relations.side_effect = [
            [{'type': 'GOB.String'}],
            [{'type': 'GOB.Integer'}],
            [{'type': 'GOB.VeryManyReference'}],
            [{'type': 'GOB.String'}],
            None
        ]
        msg = {
            'header': {
                'catalogue': 'catalog',
                'jobid': 20,
                'stepid': 240,
            }
        }

        _split_job(msg)

        self.assertSplitJobsPublished(mock_connection, [
            self._get_split_msg(msg, 'catalog', 'collA', 'attrA'),
            self._get_split_msg(msg, 'catalog', 'collA', 'attrB'),
            self._get_split_msg(msg, 'catalog', 'dst_col', 'attr'),
        ])
        
        msg = {
            'header': {
                'catalogue': 'catalog',
                'collection': 'dst_col',
                'jobid': 20,
                'stepid': 240,
            }
        }
        _split_job(msg)
        mock_logger.info.assert_called_with("Missing relation specification for catalog dst_col attr. Skipping")

    @patch("gobupload.relate.datetime")
    @patch("gobupload.relate.GOBModel")
    @patch("gobupload.relate.get_relation_name")
    def test_build_relations(self, mock_get_relation_name, mock_model, mock_datetime):
        mock_datetime.datetime.utcnow.return_value.isoformat.return_value = 'DATETIME'
        msg = {
            'header': {
                'catalogue': 'catalog',
                'collection': 'collection',
                'attribute': 'attribute',
            }
        }

        result = build_relations(msg)

        expected_result = {
            'header': {
                'version': '0.1',
                'source': 'GOB',
                'application': 'GOBRelate',
                'entity': mock_get_relation_name.return_value,
                'timestamp': 'DATETIME',
                'process_id': 'DATETIME.GOBRelate.catalog.collection.attribute',
                'catalogue': 'rel',
                'collection': mock_get_relation_name.return_value,
                'attribute': 'attribute',
                'original_catalogue': 'catalog',
                'original_collection': 'collection',
                'original_attribute': 'attribute',
            },
        }

        self.assertEqual(expected_result, result)
        mock_get_relation_name.assert_called_with(mock_model.return_value, 'catalog', 'collection', 'attribute')

    @patch("gobupload.relate.datetime")
    @patch("gobupload.relate.publish_result")
    @patch("gobupload.relate._split_job")
    def test_build_relations_split(self, mock_split_job, mock_publish, mock_datetime):
        mock_datetime.datetime.utcnow.return_value.isoformat.return_value = 'DATETIME'
        msg = {
            'header': {
                'catalogue': 'catalog',
                'collection': 'collection',
            }
        }

        split_msg = {
            'header': {
                'version': '0.1',
                'source': 'GOB',
                'application': 'GOBRelate',
                'entity': 'collection',
                'timestamp': 'DATETIME',
                'process_id': 'DATETIME.GOBRelate.catalog.collection',
                'catalogue': 'catalog',
                'collection': 'collection',
                'is_split': True,
            }
        }

        self.assertEqual(mock_publish.return_value, build_relations(msg))
        mock_split_job.assert_called_with(split_msg)

        mock_publish.assert_called_with(split_msg, [])

    @patch("gobupload.relate.MaterializedViews")
    def test_get_materialized_view_by_relation_name(self, mock_mv):
        self.assertEqual(mock_mv.return_value.get_by_relation_name.return_value,
                         _get_materialized_view_by_relation_name('relation name'))
        mock_mv.return_value.get_by_relation_name.assert_called_with('relation name')

        mock_mv.return_value.get_by_relation_name.side_effect = Exception
        with self.assertRaises(GOBException):
            _get_materialized_view_by_relation_name('relation name')

    @patch("gobupload.relate.MaterializedViews")
    @patch("gobupload.relate._get_materialized_view_by_relation_name")
    def test_get_materialized_view(self, mock_get_mv, mock_mv):
        self.assertEqual(mock_get_mv.return_value, _get_materialized_view('rel', 'coll', 'attr'))

        with self.assertRaises(GOBException):
            _get_materialized_view('cat', None, 'attr')

        with self.assertRaises(GOBException):
            _get_materialized_view('cat', 'coll', None)

        self.assertEqual(mock_mv.return_value.get.return_value, _get_materialized_view('cat', 'col', 'attr'))
        mock_mv.return_value.get.side_effect = Exception

        with self.assertRaises(GOBException):
            _get_materialized_view('cat', 'col', 'attr')

    @patch("gobupload.relate.datetime")
    @patch("gobupload.relate._get_materialized_view")
    @patch("gobupload.relate.GOBStorageHandler")
    def test_update_materialized_view(self, mock_storage_handler, mock_get_mv, mock_datetime):
        mock_datetime.datetime.utcnow.return_value.isoformat.return_value = 'DATETIME'

        msg = {
            'header': {
                'catalogue': 'catalog',
                'collection': 'collection',
                'attribute': 'attribute',
            }
        }

        expected_result_msg = {
            'header': {
                'catalogue': 'catalog',
                'collection': 'collection',
                'attribute': 'attribute',
                'timestamp': 'DATETIME',
                'process_id': 'DATETIME.GOBRelate.catalog.collection',
            }
        }

        self.assertEqual(expected_result_msg, update_materialized_view(msg))

        mock_get_mv.assert_called_with('catalog', 'collection', 'attribute')
        mock_get_mv.return_value.refresh.assert_called_with(mock_storage_handler.return_value)
