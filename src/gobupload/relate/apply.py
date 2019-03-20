"""
Module that contains logic to apply new relation data on the current entities

"""
from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobcore.utils import ProgressTicker

from gobupload.storage.relate import get_current_relations, RelationUpdater


def update_row_relation(row, relation, field_name, field_type):
    """
    Update a current relation (row) with new values (relation)

    :param row:
    :param relation:
    :param field_name:
    :param field_type:
    :return: True if any change in relation data has been found
    """
    is_changed = False

    dst = relation['dst']
    if field_type == 'GOB.Reference':
        # {'bronwaarde': 'xxx'} => {'bronwaarde': 'xxx', '_id': '123'}
        assert len(dst) == 1, f"Error: Single reference with multiple values ({len(dst)})"
        id = dst[0]['id']
        is_changed = row[field_name].get(FIELD.ID) != id
        row[field_name] = {
            **row[field_name],
            FIELD.ID: id
        }
    else:
        # Example:
        # ROW = {
        #     field_name: [{'bronwaarde': 'B'}, {'bronwaarde': 'A'}]
        # }
        # DST = {
        #     'dst': [{'id': '13588001', 'bronwaardes': ['A']},
        #             {'id': '12789002', 'bronwaardes': ['B']}]
        # }
        # =>
        # ROW = {
        #     field_name: [{'bronwaarde': 'B', _id='12789002'}, {'bronwaarde': 'A', _id='13588001'}]
        # }
        ids = {}
        for item in dst:
            for bronwaarde in item['bronwaardes']:
                ids[bronwaarde] = item

        for item in row[field_name]:
            id = ids.get(item["bronwaarde"])
            if id is None:
                is_changed = is_changed or item.get(FIELD.ID, False) is not None
            else:
                id = id['id']
                is_changed = is_changed or item.get(FIELD.ID) != id
            item[FIELD.ID] = id

    return is_changed


def clear_row_relation(row, field_name, field_type):
    """
    Clears current relation data (row) by explicitly setting _id to None

    :param row:
    :param field_name:
    :param field_type:
    :return: True if any change in relation data has been found
    """
    is_changed = False

    if field_type == 'GOB.Reference':
        # {'bronwaarde': 'xxx'} => {'bronwaarde': 'xxx', _id: None}
        is_changed = row[field_name].get(FIELD.ID, False) is not None
        row[field_name] = {
            **row[field_name],
            FIELD.ID: None
        }
    else:
        # [{'bronwaarde': 'xxx'}, ...] => [{'bronwaarde': 'xxx', '_id': None}, ...]
        for item in row[field_name]:
            is_changed = is_changed or item.get(FIELD.ID, False) is not None
            item[FIELD.ID] = None

    return is_changed


def prepare_row(row, field_name, field_type):
    """
    If the reference field is None then set it to an empty object or list

    :param row:
    :param field_name:
    :param field_type:
    :return:
    """
    if row[field_name] is None:
        if field_type == 'GOB.Reference':
            row[field_name] = {}
        else:
            row[field_name] = []


def get_next_item(items):
    """
    Get next item from any iterable, return None if no next item exists

    :param items:
    :return:
    """
    try:
        return next(items)
    except StopIteration:
        return None


def get_match(current_relation, relation):
    """
    Tells if current and new relation data match on source and id (src_id) and on seq nr

    :param current_relation:
    :param relation:
    :return:
    """
    if current_relation is None or relation is None:
        match_srcid = False
        match_seqnr = False
    else:
        src = relation['src']
        # Skip 'intermediate' relations
        match_srcid = src['source'] == current_relation[FIELD.SOURCE] and src['id'] == current_relation[FIELD.ID]
        # Match on sequence. Take last relation for every sequence
        match_seqnr = (src["volgnummer"] is None and relation["eind_geldigheid"] is None) or \
                      (src['volgnummer'] == current_relation.get(FIELD.SEQNR) and
                       relation["eind_geldigheid"] == current_relation.get(FIELD.END_VALIDITY))

    return match_srcid, match_seqnr


def _get_field_type(catalog_name, collection_name, field_name):
    """
    Get the field type for the given field_name in the catalog and collection

    :param catalog_name:
    :param collection_name:
    :param field_name:
    :return:
    """
    model = GOBModel()
    collection = model.get_collection(catalog_name, collection_name)
    field = collection['all_fields'][field_name]
    field_type = field['type']
    assert field_type in ["GOB.Reference", "GOB.ManyReference"], f"Error: unexpected field type {field_type}"
    return field_type


def match_relation(current_relation, relation, field_name, field_type):
    """
    Match current and new relation for the given field name

    :param current_relation:
    :param relation:
    :param field_name:
    :param field_type:
    :return:
    """
    if current_relation is None:
        assert relation is None
        is_changed = False
        next_current_relation = False
        next_relation = False
    else:
        match_srcid, match_seqnr = get_match(current_relation=current_relation, relation=relation)

        full_match = match_srcid and match_seqnr
        partly_match = match_srcid and not match_seqnr

        next_current_relation = not partly_match  # Wait for next relation on partly match
        next_relation = True  # Always get next (new) relation

        prepare_row(current_relation, field_name, field_type)

        if full_match:
            is_changed = update_row_relation(current_relation, relation, field_name, field_type)
        elif partly_match:
            is_changed = False
        else:
            is_changed = clear_row_relation(current_relation, field_name, field_type)

    return is_changed, next_current_relation, next_relation


def apply_relations(catalog_name, collection_name, field_name, relations):
    """
    Register the current relation to current entity

    :param catalog_name:
    :param collection_name:
    :param field_name:
    :param relations:
    :return: None
    """
    field_type = _get_field_type(catalog_name, collection_name, field_name)

    current_relations = get_current_relations(catalog_name, collection_name, field_name)
    relations_iter = iter(relations)

    updater = RelationUpdater(catalog_name, collection_name)

    next_current_relation = True
    next_relation = True
    progress = ProgressTicker("Update relations", 10000)
    while next_current_relation or next_relation:
        progress.tick()

        if next_current_relation:
            current_relation = get_next_item(current_relations)

        if next_relation:
            relation = get_next_item(relations_iter)

        is_changed, next_current_relation, next_relation = match_relation(
            current_relation, relation, field_name, field_type)

        if is_changed:
            updater.update(field_name, current_relation)

    updater.completed()
