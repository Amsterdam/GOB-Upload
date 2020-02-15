from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobupload.relate.table.entrypoint import _check_message, relate_table_src_message_handler, GOBException


class MockModel:
    model = {
        'the catalog': {
            'the collection': {
                'the attribute': 'val',
            }
        }
    }

    def get_catalog(self, catalog):
        return self.model.get(catalog)

    def get_collection(self, catalog, collection):
        return self.model.get(catalog, {}).get(collection)


class MockSources(MockModel):
    def get_field_relations(self, catalog, collection, attribute):
        return self.model.get(catalog, {}).get(collection, {}).get(attribute)


class TestEntrypoint(TestCase):

    @patch("gobupload.relate.table.entrypoint.GOBModel", MockModel)
    @patch("gobupload.relate.table.entrypoint.GOBSources", MockSources)
    def test_check_message(self):
        msg = {
            'header': {
                'original_catalogue': 'the catalog',
                'original_collection': 'the collection',
                'original_attribute': 'the attribute',
            }
        }

        # Message ok. No errors
        _check_message(msg)

        # Remove headers and/or change to invalid value
        for key in msg['header'].keys():
            new_header = msg['header'].copy()

            # Invalid catalog/collection/attribute
            new_header[key] = 'invalid value'
            with self.assertRaises(GOBException):
                _check_message({'header': new_header})

            # Missing header key
            del new_header[key]
            with self.assertRaises(GOBException):
                _check_message(({'header': new_header}))

    @patch("gobupload.relate.table.entrypoint.get_relation_name", lambda m, cat, col, field: f"{cat}_{col}_{field}")
    @patch("gobupload.relate.table.entrypoint._check_message")
    @patch("gobupload.relate.table.entrypoint.RelationTableRelater")
    def test_relate_table_src_message_handler(self, mock_relater, mock_check_message):
        msg = {
            'header': {
                'original_catalogue': 'the catalog',
                'original_collection': 'the collection',
                'original_attribute': 'the attribute',
            },
            'timestamp': 'the timestamp',
        }
        mock_relater.return_value.update.return_value = ('result filename', 2840)

        result = relate_table_src_message_handler(msg)
        mock_check_message.assert_called_with(msg)

        mock_relater.assert_called_with('the catalog', 'the collection', 'the attribute')

        self.assertEqual({
            'header': {
                'original_catalogue': 'the catalog',
                'original_collection': 'the collection',
                'original_attribute': 'the attribute',
                'catalogue': 'rel',
                'collection': 'the catalog_the collection_the attribute',
                'entity': 'the catalog_the collection_the attribute',
                'source': 'GOB',
                'application': 'GOB',
                'version': '0.1',
                'timestamp': 'the timestamp',
            },
            'summary': {
                'warnings': [],
                'errors': [],
            },
            'contents_ref': 'result filename',
            'confirms': 2840,
        }, result)

