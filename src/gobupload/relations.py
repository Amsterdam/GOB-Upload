'''
Build relations for a specific collection from a catalog.

We retrieve all relations for each source and create the relation on the
_id field.

'''
from sqlalchemy import cast
from sqlalchemy.types import Text

from gobcore.model import GOBModel
from gobcore.sources import GOBSources

from gobcore.model.sa.gob import models
from gobupload.storage.handler import GOBStorageHandler

model = GOBModel()
sources = GOBSources()
storage = None


# TODO: see if alignment with sqlalchemy is possible
OPERATORS = {
    'is_null': 'IS NULL'
}


def build_relations(msg):
    """Build all relations for a catalog.

    :param msg: a message from the broker containing the catalog to build relations to
    :return:
    """
    global storage

    storage = GOBStorageHandler()

    catalog_name = msg['catalogue']

    for collection_name in model.get_collection_names(catalog_name):
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

        result = storage.session.execute(query) if query else None

    if result:
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


def _equals(catalog_name, collection_name, relation):
    """Build an update query based on a direct relation between 'bronwaarde' and a field in the related object

    :param catalog_name: the source catalog we are relating to
    :param collection_name: the source collection we are relating to
    :param relation: the relation dict, containing all details about the relation
    :return:
    """
    update_table = model.get_table_name(relation['catalog'], relation['collection'])
    source_table = model.get_table_name(catalog_name, collection_name)

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


def _through(catalog_name, collection_name, relation):
    """Build an update query based on a chain through entities

    :param update_table: the table_name of the collection we are about to update
    :param source_table: the table_name of the collection we relating to
    :param relation: the relation dict, containing all details about the relation
    :return:
    """
    if relation['type'] == 'GOB.ManyReference':
        # Not implemented yet, return None to skip query execution.
        return None

    update_table = model.get_table_name(relation['catalog'], relation['collection'])

    # Get the last table in the chain
    final_table = model.get_table_name(*relation['chain'][-1]['entity'].split(':'))

    # Build the joins and filters using the chains in the relation
    joins, filters = _expand_joins_and_filters(update_table, relation['chain'])

    joins_str = ' '.join(joins)

    query = f"""
UPDATE {update_table}
SET {relation['field_name']} = enhanced.related
FROM (
    SELECT {update_table}._id, (\'{{"id": "\'|| {final_table}._id ||\'"}}\')::JSONB as related
    FROM {update_table}
    {joins_str}
"""
    if filters:
        filters_str = ' AND '.join(filters)
        query += f"""
    WHERE {filters_str}
"""
    query += f"""
) AS enhanced
WHERE {update_table}._source = '{relation['source']}'
AND {update_table}._id = enhanced._id
"""
    return query


def _expand_joins_and_filters(update_table, chain):
    joins = []
    filters = []

    # Start with the update_table in the chain
    chain_table = update_table
    for item in chain:
        chain_catalog, chain_collection = item['entity'].split(':')
        # Set the source_table to the next item in the chain
        source_table = model.get_table_name(chain_catalog, chain_collection)
        joins.append(f"LEFT JOIN {source_table} ON \
            {chain_table}.{item['source_attribute']}->>'id' = {source_table}._id")
        # Set the chain_table to the item we've just handled
        chain_table = model.get_table_name(chain_catalog, chain_collection)

        # Add the filters
        if 'filters' in item:
            for filter in item['filters']:
                filters.append(f"{chain_table}.{filter['field']} {OPERATORS[filter['op']]}")

    return joins, filters


def _get_update_query(catalog_name, collection_name, relation):
    """Build an update query based on a direct relation between 'bronwaarde' and a field in the related object

    :param update_table: the table_name of the collection we are about to update
    :param source_table: the table_name of the collection we relating to
    :param relation: the relation dict, containing all details about the relation
    :return:
    """

    methods = {
        'equals': _equals,
        'geo_in': _geo_in,
        'through': _through,
    }
    return methods[relation['method']](catalog_name, collection_name, relation)
