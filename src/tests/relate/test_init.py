from unittest import TestCase
from unittest.mock import MagicMock, patch, call, ANY

from gobcore.exceptions import GOBException

from gobupload import gob_model
from gobupload.relate import prepare_relate, check_relation, _log_exception, _split_job
from gobupload.relate import update_materialized_view, _get_materialized_view_by_relation_name
from gobupload.relate import _get_materialized_view, WORKFLOW_EXCHANGE, WORKFLOW_REQUEST_KEY
from gobupload.relate import process_relate, verify_process_message, get_catalog_from_msg
from gobupload.relate import CATALOG_KEY, COLLECTION_KEY, ATTRIBUTE_KEY
from gobupload.storage.handler import StreamSession


mock_logger = MagicMock()


@patch('gobupload.relate.logger', mock_logger)
class TestInit(TestCase):

    class MockModel:
        model = {
            'catalog': {
                'collections': {
                    'collA': {
                        'attributes': ['attrA', 'attrB', 'attrC'],
                        'references': ['attrA', 'attrB', 'attrC'],
                    },
                    'colB': {
                        'attributes': [],
                        'references': [],
                    },
                    'dst_col': {
                        'attributes': ['attr'],
                        'references': ['attr'],
                    },
                    'the collection': {
                        'the attribute': 'val',
                        'attributes': [],
                        'references': [],
                    }
                }
            }
        }

        def __getitem__(self, catalog):
            return self.model[catalog]

        def get(self, catalog):
            return self.model.get(catalog)

    class MockSources:
        def __init__(self, gobmodel):
            self.gobmodel = gobmodel

        def get_field_relations(self, catalog, collection, attribute):
            return self.gobmodel.model.get(catalog, {}).get(
                'collections').get(collection, {}).get(attribute)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('gobupload.relate.gob_model', MagicMock(spec_set=gob_model))
    @patch('gobupload.relate._log_exception')
    def test_check_relation_with_invalid_messages(self, mock_log_exception):
        """Test check_relation with invalid messages."""
        no_header = {}
        with self.assertRaises(GOBException):
            check_relation(no_header)
        mock_log_exception.assert_called_with(
            "Invalid message: header key is missing", ANY)
        mock_log_exception.reset_mock()

        invalid_catalog = {
            'header': {
                'catalogue': 'invalid catalog key'
            }
        }
        with self.assertRaises(GOBException):
            check_relation(invalid_catalog)
        mock_log_exception.assert_called()
        mock_log_exception.reset_mock()

        invalid_collection = {
            'header': {
                CATALOG_KEY: 'valid catalogue key',
                'collection': 'invalid collection key'
            }
        }
        with self.assertRaises(GOBException):
            check_relation(invalid_collection)
        mock_log_exception.assert_called()
        mock_log_exception.reset_mock()

        invalid_attribute = {
            'header': {
                CATALOG_KEY: 'valid catalogue key',
                COLLECTION_KEY: 'valid collection key',
                'attribute': 'invalid attribute key'
            }
        }
        with self.assertRaises(GOBException):
            check_relation(invalid_attribute)
        mock_log_exception.assert_called()

    @patch('gobupload.relate._log_exception')
    @patch('gobupload.relate.gob_model', spec_set=True, name="gob_model")
    @patch('gobupload.relate.logger', MagicMock())
    @patch('gobupload.relate.check_relations')
    @patch('gobupload.relate.check_relation_conflicts', MagicMock())
    def test_check_relation(self, mock_check_relations, mock_model, mock_log_exception):
        """Test check_relation with valid message."""
        valid_message = {
            'header': {
                CATALOG_KEY: 'valid catalogue key',
                COLLECTION_KEY: 'valid collection key',
                ATTRIBUTE_KEY: 'valid attribute key'
            }
        }

        # Check valid message result.
        result = check_relation(valid_message)
        self.assertEqual(result, {
            'header': valid_message['header'],
            'summary': ANY,
            'contents': None
        })

        # CATALOG_KEY missing.
        mock_model.__getitem__.side_effect = KeyError
        with self.assertRaises(GOBException):
            check_relation(valid_message)
        mock_model.reset_mock(side_effect=True)

        # COLLECTION_KEY missing.
        mock_model.__getitem__.return_value = {
                'collections': {}
                }
        with self.assertRaises(GOBException):
            check_relation(valid_message)
        mock_model.reset_mock(return_value=True)

        # attribute check failure
        mock_check_relations.side_effect = Exception
        result = check_relation(valid_message)
        mock_log_exception.assert_called()

    def test_log_exception(self):
        mock_logger.error = MagicMock()
        mock_logger.error.assert_not_called()
        _log_exception("any msg", "any err")
        mock_logger.error.assert_called_with("any msg: any err")
        _log_exception("any msg", "any err", 5)
        mock_logger.error.assert_called_with("any m...")

    def _get_split_msg(self, original_msg, catalog, collection, attribute):
        header = dict(original_msg['header'].items())

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

    @patch("gobupload.relate.gob_model", MockModel())
    def test_get_catalog_from_msg(self):
        "Test get_catalog_from_msg."""
        no_header_msg = {'catalogue': 'catalog'}
        with self.assertRaises(GOBException):
            get_catalog_from_msg(no_header_msg, 'catalogue')

        wrong_catalog_msg = {
            'header': {
                'catalog': 'do_you_mean_original_catalogue',
            }
        }
        with self.assertRaises(GOBException):
            get_catalog_from_msg(wrong_catalog_msg, CATALOG_KEY)

        invalid_catalogue_msg = {
            'header': {
                'catalogue': 'missing_catalogue',
            }
        }
        with self.assertRaises(GOBException):
            get_catalog_from_msg(invalid_catalogue_msg, 'catalogue')

    @patch("gobupload.relate.gob_model", MockModel())
    @patch("gobupload.relate.GOBSources")
    @patch("gobupload.relate.MessageBrokerConnection")
    def test_split_job(self, mock_connection, mock_sources):
        msg = {
            'header': {
                'catalogue': 'catalog',
                'collection': 'wrong_collection',
            }
        }
        with self.assertRaises(GOBException):
            _split_job(msg)

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
        mock_logger.warning.assert_called_with(
            "Missing relation specification for catalog dst_col attr. Skipping"
        )

    @patch("gobupload.relate.datetime")
    @patch("gobupload.relate.gob_model", new_callable=MockModel)
    @patch("gobupload.relate.get_relation_name")
    def test_prepare_relate(self, mock_get_relation_name, mock_model, mock_datetime):
        mock_datetime.datetime.utcnow.return_value.isoformat.return_value = 'DATETIME'
        msg = {
            'header': {
                'catalogue': 'catalog',
                'collection': 'collection',
                'attribute': 'attribute',
            }
        }

        result = prepare_relate(msg)

        expected_result = {
            'header': {
                'version': '0.1',
                'source': 'GOB',
                'application': 'GOBRelate',
                'entity': mock_get_relation_name.return_value,
                'timestamp': 'DATETIME',
                'catalogue': 'rel',
                'collection': mock_get_relation_name.return_value,
                'attribute': 'attribute',
                CATALOG_KEY: 'catalog',
                COLLECTION_KEY: 'collection',
                ATTRIBUTE_KEY: 'attribute',
            },
        }

        self.assertEqual(expected_result, result)
        mock_get_relation_name.assert_called_with(mock_model, 'catalog', 'collection', 'attribute')

    @patch("gobupload.relate.datetime")
    @patch("gobupload.relate.publish_result")
    @patch("gobupload.relate._split_job")
    def test_prepare_relate_split(self, mock_split_job, mock_publish, mock_datetime):
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
                'catalogue': 'catalog',
                'collection': 'collection',
                'is_split': True,
            }
        }

        self.assertEqual(mock_publish.return_value, prepare_relate(msg))
        mock_split_job.assert_called_with(split_msg)

        mock_publish.assert_called_with(split_msg, [])

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
            }
        }

        self.assertEqual(expected_result_msg, update_materialized_view(msg))

        mock_get_mv.assert_called_with('catalog', 'collection', 'attribute')
        mock_get_mv.return_value.refresh.assert_called_with(mock_storage_handler.return_value)

    @patch("gobupload.relate.gob_model", MockModel())
    @patch("gobupload.relate.GOBSources", MockSources)
    def test_verify_process_message(self):
        msg = {
            'header': {
                CATALOG_KEY: 'catalog',
                COLLECTION_KEY: 'the collection',
                ATTRIBUTE_KEY: 'the attribute',
            }
        }

        # Message ok. No errors
        verify_process_message(msg)

        # Remove headers and/or change to invalid value
        for key in msg['header']:
            new_header = msg['header'].copy()

            # Invalid catalog/collection/attribute
            new_header[key] = 'invalid value'
            with self.assertRaises(GOBException):
                verify_process_message({'header': new_header})

            # Missing header key
            del new_header[key]
            with self.assertRaises(GOBException):
                verify_process_message({'header': new_header})

    @patch("gobupload.relate.GOBStorageHandler")
    @patch("gobupload.relate.get_relation_name", lambda m, cat, col, field: f"{cat}_{col}_{field}")
    @patch("gobupload.relate.verify_process_message")
    @patch("gobupload.relate.Relater")
    def test_process_relate(self, mock_relater, mock_verify_process_message, mock_storage):
        mock_session = MagicMock(spec=StreamSession)
        mock_storage.return_value.get_session.return_value.__enter__.return_value = mock_session

        msg = {
            'header': {
                CATALOG_KEY: 'catalog',
                COLLECTION_KEY: 'the collection',
                ATTRIBUTE_KEY: 'the attribute',
            },
            'timestamp': 'the timestamp',
        }
        mock_relater.return_value.__enter__.return_value.update.return_value = 'result filename'

        result = process_relate(msg)
        mock_verify_process_message.assert_called_with(msg)

        mock_relater.assert_called_with(mock_session, 'catalog', 'the collection', 'the attribute')
        mock_relater().__enter__().update.assert_called_with(False)

        assert result == {
            'header': {
                CATALOG_KEY: 'catalog',
                COLLECTION_KEY: 'the collection',
                ATTRIBUTE_KEY: 'the attribute',
                'catalogue': 'rel',
                'collection': 'catalog_the collection_the attribute',
                'entity': 'catalog_the collection_the attribute',
                'source': 'GOB',
                'application': 'GOB',
                'version': '0.1',
                'timestamp': 'the timestamp',
            },
            'summary': mock_logger.get_summary(),
            'contents_ref': 'result filename',
        }

        # Full relate forced
        msg['header']['mode'] = 'full'
        result = process_relate(msg)
        mock_relater().__enter__().update.assert_called_with(True)
