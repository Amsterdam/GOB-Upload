import logging

from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.model import GOBModel
from gobcore.sources import GOBSources

from gobupload import relations
from gobupload.models.model import models
from gobupload.storage.handler import GOBStorageHandler


class TestRelations(TestCase):

    def setUp(self):
        # Disable logging to prevent test from connecting to RabbitMQ
        logging.disable(logging.CRITICAL)

        self.mock_sources = GOBSources()
        self.mock_relation = {
            'catalog': 'catalog',
            'collection': 'collection',
            'source': 'source',
            'field_name': 'field_name',
            'type': 'GOB.Reference',
            'method': 'equals',
            'destination_attribute': 'identificatie'
        }

        self.mock_many_relation = {
            'catalog': 'catalog',
            'collection': 'collection',
            'source': 'source',
            'field_name': 'field_name',
            'type': 'GOB.ManyReference',
            'method': 'equals',
            'destination_attribute': 'identificatie'
        }

        self.mock_entities = MagicMock()
        self.mock_entities.return_value.count = 10

    def tearDown(self):
        logging.disable(logging.NOTSET)

    @patch('gobupload.relations._update_relation')
    @patch('gobupload.relations.GOBSources.get_relations')
    def test_build_relations(self, mock_get_relations, mock_update_relations):
        mock_get_relations.return_value = [self.mock_relation]

        relations.build_relations('catalog', 'collection')

        mock_update_relations.assert_called_with('catalog', 'collection', self.mock_relation)

    @patch('gobupload.relations.storage')
    @patch('gobupload.relations._get_all_entities')
    @patch('gobupload.relations.models')
    def test_update_relation(self, mock_models, mock_get_all_entities, mock_storage):
        mock_models.__getitem__.return_value = 'CollectionModel'
        mock_get_all_entities.return_value = self.mock_entities

        relations._update_relation('catalog', 'collection', self.mock_relation)

        # Assert _get_all_entities is called
        mock_get_all_entities.assert_called_with('CollectionModel', self.mock_relation)

        # Assert count is called
        self.mock_entities.count.assert_called()

        expected_query = f"""
UPDATE catalog_collection
SET field_name = field_name::JSONB ||
                               ('{{\"id\": \"'|| catalog_collection._id ||'\"}}')::JSONB
FROM catalog_collection
WHERE field_name->>'bronwaarde' = catalog_collection.identificatie
AND catalog_collection._source = 'source'
"""
        mock_storage.session.execute.assert_called_with(expected_query)

    @patch('gobupload.relations.storage')
    def test_get_all_entities_for_reference(self, mock_storage):
        mock_model = MagicMock()
        mock_model.field_name = {'bronwaarde': MagicMock()}

        relations._get_all_entities(mock_model, self.mock_relation)
        mock_storage.session.query.assert_called()

    @patch('gobupload.relations.storage')
    def test_get_all_entities_for_many_reference(self, mock_storage):
        mock_model = MagicMock()
        mock_model.field_name = {'bronwaarde': MagicMock()}

        relations._get_all_entities(mock_model, self.mock_many_relation)
        mock_storage.session.query.assert_called()

    def test_equals_for_reference(self):
        query = relations._equals('catalog_collection', 'catalog2_collection2', self.mock_relation)

        expected_query = f"""
UPDATE catalog_collection
SET field_name = field_name::JSONB ||
                               ('{{\"id\": \"'|| catalog2_collection2._id ||'\"}}')::JSONB
FROM catalog2_collection2
WHERE field_name->>'bronwaarde' = catalog2_collection2.identificatie
AND catalog_collection._source = 'source'
"""

        self.assertEqual(query, expected_query)

    def test_equals_for_many_reference(self):
        query = relations._equals('catalog_collection', 'catalog2_collection2', self.mock_many_relation)

        expected_query = f"""
UPDATE catalog_collection
SET field_name = enhanced.related
FROM (
    SELECT catalog_collection._id, jsonb_agg(value::JSONB ||
                               ('{{\"id\": \"'|| catalog2_collection2._id ||'\"}}')::JSONB) as related
    FROM catalog_collection, json_array_elements(catalog_collection.field_name)
    LEFT JOIN catalog2_collection2
    ON value->>'bronwaarde' = catalog2_collection2.identificatie
    GROUP BY catalog_collection._id
) AS enhanced
WHERE catalog_collection._source = 'source'
AND catalog_collection._id = enhanced._id
"""

    def test_geo_in(self):
        relations._geo_in('catalog_collection', 'catalog2_collection2', self.mock_many_relation)

    @patch('gobupload.relations._geo_in')
    @patch('gobupload.relations._equals')
    def test_get_update_query(self, mock_equals, mock_geo_in):
        relations._get_update_query('catalog', 'collection', self.mock_relation)
        mock_equals.assert_called_with('catalog_collection', 'catalog_collection', self.mock_relation)

        self.mock_relation['method'] = 'geo_in'
        relations._get_update_query('catalog', 'collection', self.mock_relation)
        mock_geo_in.assert_called_with('catalog_collection', 'catalog_collection', self.mock_relation)
