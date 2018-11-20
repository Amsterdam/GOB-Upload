'''
Build relations for a specific collection from a catalog.

We retrieve all relations for each source and create the relation on the
_id field.

'''
from sqlalchemy import cast
from sqlalchemy.types import Text

from gobcore.model import GOBModel
from gobcore.sources import GOBSources

from gobupload.models.model import models
from gobupload.storage.handler import GOBStorageHandler

model = GOBModel()
sources = GOBSources()
storage = GOBStorageHandler()


def build_relations(catalog_name, collection_name):
    """Build all relations for a collection from a catalog.

    :param catalog_name: the name of the catalog
    :param collection_name: the name of the collection
    :return:
    """
    relations = sources.get_relations(catalog_name, collection_name)
    for relation in relations:
        _update_relation(catalog_name, collection_name, relation)


def _update_relation(catalog_name, collection_name, relation):
    """Update a specific relation for a collection from a catalog.

    :param catalog_name: the name of the catalog
    :param collection_name: the name of the collection
    :param relation: the relation dict, containing all details about the relation
    :return:
    """
    with storage.get_session():
        # Get all objects we need to update
        update_model = models[model.get_table_name(relation['catalog'], relation['collection'])]

        # Get all entities which can be updated
        entities = _get_all_entities(update_model, relation)
        total_count = entities.count()
        print(f"About to build {total_count} relation(s) for "
              f"{relation['field_name']} on {relation['catalog']}_{relation['collection']} "
              f"to {relation['destination_attribute']} on {catalog_name}_{collection_name}")

        query = _get_update_query(catalog_name, collection_name, relation)
        if query:
            result = storage.session.execute(query)

            print(f"{result.rowcount} relation(s) made for "
                  f"{relation['field_name']} on {relation['catalog']}_{relation['collection']} "
                  f"to {relation['destination_attribute']} on {catalog_name}_{collection_name}")
        else:
            print("No relations made")


def _get_all_entities(update_model, relation):
    """Retrieve the entities for which a relation can be made.

    :param update_model: the sqlalchemy model class of the collection we are about to update
    :param relation: the relation dict, containing all details about the relation
    :return:
    """
    if relation['type'] == 'GOB.Reference':
        # Get all entities where bronwaarde has a value
        return storage.session.query(update_model).filter(
            getattr(update_model, relation['field_name'])['bronwaarde'].astext.isnot(None)
        )
    elif relation['type'] == 'GOB.ManyReference':
        # Get all entities where the value is an empty array
        return storage.session.query(update_model).filter(
            cast(getattr(update_model, relation['field_name']), Text) != '[]'
        )


def _equals(update_table, source_table, relation):
    """Build an update query based on a direct relation between 'bronwaarde' and a field in the related object

    :param update_table: the table_name of the collection we are about to update
    :param source_table: the table_name of the collection we relating to
    :param relation: the relation dict, containing all details about the relation
    :return:
    """
    if relation['type'] == 'GOB.Reference':
        return f"""
UPDATE {update_table}
SET {relation['field_name']} = {relation['field_name']}::JSONB ||
                               ('{{\"id\": \"'|| {source_table}._id ||'\"}}')::JSONB
FROM {source_table}
WHERE {relation['field_name']}->>'bronwaarde' = {source_table}.{relation['destination_attribute']}
AND {update_table}._source = '{relation['source']}'
"""
    elif relation['type'] == 'GOB.ManyReference':
        return f"""
UPDATE {update_table}
SET {relation['field_name']} = enhanced.related
FROM (
    SELECT {update_table}._id, jsonb_agg(value::JSONB ||
                               ('{{\"id\": \"'|| {source_table}._id ||'\"}}')::JSONB) as related
    FROM {update_table}, json_array_elements({update_table}.{relation['field_name']})
    LEFT JOIN {source_table}
    ON value->>'bronwaarde' = {source_table}.{relation['destination_attribute']}
    GROUP BY {update_table}._id
) AS enhanced
WHERE {update_table}._source = '{relation['source']}'
AND {update_table}._id = enhanced._id
"""


def _geo_in(catalog_name, collection_name, relation):
    """Build an update query based on a geographic lookup

    Not implemented yet, return None to skip query execution.

    :param update_table: the table_name of the collection we are about to update
    :param source_table: the table_name of the collection we relating to
    :param relation: the relation dict, containing all details about the relation
    :return:
    """
    return None


def _get_update_query(catalog_name, collection_name, relation):
    """Build an update query based on a direct relation between 'bronwaarde' and a field in the related object

    :param update_table: the table_name of the collection we are about to update
    :param source_table: the table_name of the collection we relating to
    :param relation: the relation dict, containing all details about the relation
    :return:
    """
    update_table = model.get_table_name(relation['catalog'], relation['collection'])
    source_table = model.get_table_name(catalog_name, collection_name)

    methods = {
        'equals': _equals,
        'geo_in': _geo_in,
    }
    return methods[relation['method']](update_table, source_table, relation)
