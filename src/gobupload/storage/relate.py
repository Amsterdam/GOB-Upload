"""
Module contains the storage related logic for GOB Relations

"""
import datetime

from gobupload.storage.handler import GOBStorageHandler

from gobcore.logging.logger import logger
from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobcore.model.relations import get_relation_name
from gobcore.quality.issue import QA_CHECK, QA_LEVEL, Issue, log_issue

from gobupload.relate.update import Relater
from gobupload.storage.execute import _execute

# Dates compare at start of day
_START_OF_DAY = datetime.time(0, 0, 0)

# Match for destination match fields
DST_MATCH_PREFIX = "dst_match_"

# SQL Query parts
JOIN = "join"
WHERE = "where"

# comparison types
EQUALS = "equals"     # equality comparison, eg src.bronwaarde == dst.code
LIES_IN = "lies_in"   # geometric comparison, eg src.geometrie lies_in dst_geometrie

# Maximum number of error messages to report
_MAX_RELATION_CONFLICTS = 25


def date_to_datetime(value):
    """
    Convert a date value to a datetime value

    :param value: a date value
    :return: The corresponding datetime value
    """
    return datetime.datetime.combine(value, _START_OF_DAY)


def _get_date_origin_fields():
    """
    Get all date fieldnames

    :return: A list of date fieldnames
    """
    DATE_FIELDS = [FIELD.START_VALIDITY, FIELD.END_VALIDITY]
    ORIGINS = ["src", "dst"]

    date_origin_fields = []
    for date_field in DATE_FIELDS:
        for origin in ORIGINS:
            date_origin_fields.append(f"{origin}_{date_field}")

    return date_origin_fields


def _convert_row(row):
    """
    Convert a database row to a dictionary.

    If any of the date fields has a datetime value, convert all date values to datetime values
    :param row: A database row
    :return: A dictionary
    """
    result = dict(row)
    date_origin_fields = _get_date_origin_fields()

    has_datetimes = True in [isinstance(result.get(field), datetime.datetime) for field in date_origin_fields]
    if has_datetimes:
        for date_origin_field in date_origin_fields:
            value = result.get(date_origin_field)
            if value and not isinstance(value, datetime.datetime):
                result[date_origin_field] = date_to_datetime(value)

    return result


def _get_data(query):
    """
    Execute the query and return the result as a list of dictionaries

    :param query:
    :return:
    """
    handler = GOBStorageHandler()
    with handler.get_session() as session:
        data = session.execute(query)
        for row in data:
            yield _convert_row(row)


def get_current_relations(catalog_name, collection_name, field_name):
    """
    Get the current relations as an iterable of dictionaries
    Each relation is transformed into a dictionary

    :param catalog_name:
    :param collection_name:
    :param field_name:
    :return: An iterable of dicts
    """
    model = GOBModel()
    table_name = model.get_table_name(catalog_name, collection_name)

    collection = model.get_collection(catalog_name, collection_name)
    field = collection['all_fields'][field_name]
    field_type = field['type']
    assert field_type in ["GOB.Reference", "GOB.ManyReference"], f"Error: unexpected field type '{field_type}'"

    select = [FIELD.GOBID, field_name, FIELD.SOURCE, FIELD.ID]
    order_by = [FIELD.SOURCE, FIELD.ID]
    if model.has_states(catalog_name, collection_name):
        select += [FIELD.SEQNR, FIELD.END_VALIDITY]
        order_by += [FIELD.SEQNR, FIELD.START_VALIDITY]
    query = f"""
SELECT   {', '.join(select)}
FROM     {table_name}
WHERE    {FIELD.DATE_DELETED} IS NULL
ORDER BY {', '.join(order_by)}
"""
    rows = _execute(query)
    for row in rows:
        row = dict(row)
        yield row


def _query_missing(query, check, attr, max_warnings=50):
    """
    Query for any missing attributes

    :param query: query to execute
    :param items_name: name of the missing attribute
    :return: None
    """
    count = 0
    historic_count = 0
    for data in _get_data(query):
        if data.get('eind_geldigheid') is None:
            # Report actual warnings
            count += 1
            if count <= max_warnings:
                # Create an issue for the failing check
                # The entity that contains the error is data, the id-attribute is named id
                # The attribute that is in error is called bronwaarde
                issue = Issue(check, data, 'id', 'bronwaarde')
                issue.attribute = attr  # Set the name of the attribute that has the failing bronwaarde
                log_issue(logger, QA_LEVEL.WARNING, issue)
        else:
            # Count historic warnings
            historic_count += 1

    items_name = f"{attr} {check['msg']}"
    if count > max_warnings:
        logger.data_warning(f"{items_name}: {count} actual warnings, reported first {max_warnings} only")
    if historic_count > 0:
        logger.data_info(f"{items_name}: {historic_count} historical errors")


def _get_relation_check_query(query_type, src_catalog_name, src_collection_name, src_field_name):
    assert query_type in ["dangling", "missing"], "Relation check query expects type to be dangling or missing"

    model = GOBModel()
    src_collection = model.get_collection(src_catalog_name, src_collection_name)
    src_table_name = model.get_table_name(src_catalog_name, src_collection_name)
    src_field = src_collection['all_fields'].get(src_field_name)
    src_has_states = model.has_states(src_catalog_name, src_collection_name)

    is_many = src_field['type'] == "GOB.ManyReference"

    relation_table_name = "rel_" + get_relation_name(model, src_catalog_name, src_collection_name, src_field_name)

    main_select = [f"src.{FIELD.ID} as id",
                   f"src.{src_field_name}->>'{FIELD.SOURCE_VALUE}' as {FIELD.SOURCE_VALUE}",
                   f"src.{FIELD.EXPIRATION_DATE}"]
    select = [FIELD.ID, f"{src_field_name}->>'{FIELD.SOURCE_VALUE}'", FIELD.EXPIRATION_DATE]

    if src_has_states:
        state_select = [FIELD.SEQNR, FIELD.START_VALIDITY, FIELD.END_VALIDITY]
        select.extend(state_select)
        main_select.extend([f"src.{field}" for field in state_select])
    select = ",\n    ".join(select)
    main_select = ",\n    ".join(main_select)

    join_on = ['src._id = rel.src_id']
    if src_has_states:
        join_on.extend(['src.volgnummer = rel.src_volgnummer'])
    join_on = " AND ".join(join_on)

    src = f"""
(
SELECT
    {select},
    _date_deleted,
    jsonb_array_elements({src_field_name}) as {src_field_name}
FROM
    {src_table_name}
) AS src
""" if is_many else f"{src_table_name} AS src"

    where = ["COALESCE(src._expiration_date, '9999-12-31'::timestamp without time zone) > NOW()"]

    # For missing relations check is bronwaarde is empty
    where.extend([f"{src_field_name}->>'bronwaarde' IS NULL"] if query_type == "missing" else [])

    # For dangling relations check if bronwaarde is filled but no destination is found
    where.extend([f"{src_field_name}->>'bronwaarde' IS NOT NULL",
                  "rel.dst_id IS NULL"] if query_type == "dangling" else [])

    where = " AND ".join(where)
    query = f"""
SELECT
    {main_select}
FROM
    {src}
JOIN {relation_table_name} rel
ON
    {join_on}
WHERE
    {where}
"""
    return query


def check_relations(src_catalog_name, src_collection_name, src_field_name):
    """
    Check relations for any dangling relations

    Dangling can be because a relation exist without any bronwaarde
    or the bronwaarde cannot be matched with any referenced entity

    :param src_catalog_name:
    :param src_collection_name:
    :param src_field_name:
    :return: None
    """

    name = f"{src_collection_name} {src_field_name}"

    missing_query = _get_relation_check_query("missing", src_catalog_name, src_collection_name, src_field_name)
    _query_missing(missing_query, QA_CHECK.Sourcevalue_exists, name)

    dangling_query = _get_relation_check_query("dangling", src_catalog_name, src_collection_name, src_field_name)
    _query_missing(dangling_query, QA_CHECK.Reference_exists, name)


def check_very_many_relations(src_catalog_name, src_collection_name, src_field_name):
    """
    Check very many relations for any dangling relations

    Dangling can be because a relation exist without any bronwaarde
    or the bronwaarde cannot be matched with any referenced entity.
    This can be checked in the relation table instead of the json
    attribute itself.

    :param src_catalog_name:
    :param src_collection_name:
    :param src_field_name:
    :return: None
    """
    # Get the source catalog, collection and field for the given names
    model = GOBModel()
    src_table_name = model.get_table_name(src_catalog_name, src_collection_name)
    src_has_states = model.has_states(src_catalog_name, src_collection_name)

    relation_table_name = "rel_" + get_relation_name(model, src_catalog_name, src_collection_name, src_field_name)

    select = ["src._id as id", "rel.bronwaarde as bronwaarde"]
    group_by = ["src._id", "rel.bronwaarde"]
    if src_has_states:
        state_select = ["src.volgnummer", "src.begin_geldigheid", "src.eind_geldigheid"]
        select.extend(state_select)
    select = ",\n    ".join(select)
    group_by = ",\n    ".join(group_by)

    join_on = ['src._id = rel.src_id']
    if src_has_states:
        join_on.extend(['src._volgnummer = rel.src_volgnummer'])
    join_on = ",\n    ".join(join_on)

    name = f"{src_collection_name} {src_field_name}"

    bronwaarden = f"""
SELECT
    {select}
FROM
    {src_table_name} src
LEFT OUTER JOIN {relation_table_name} rel
ON
    {join_on}
WHERE
    src._date_deleted IS NULL AND
    rel.bronwaarde IS NULL
GROUP BY
    {group_by}
"""
    _query_missing(bronwaarden, QA_CHECK.Sourcevalue_exists, name)

    dangling = f"""
SELECT
    {select}
FROM
    {src_table_name} src
LEFT OUTER JOIN {relation_table_name} rel
ON
    {join_on}
WHERE
    src._date_deleted IS NULL AND
    rel.bronwaarde IS NOT NULL AND
    rel.dst_id IS NULL
GROUP BY
    {group_by}
"""
    _query_missing(dangling, QA_CHECK.Reference_exists, name)


def check_relation_conflicts(catalog_name, collection_name, attribute_name):
    updater = Relater(catalog_name, collection_name, attribute_name)
    query = updater.get_conflicts_query()

    result = _execute(query, stream=True, max_row_buffer=25000)

    conflicts = 0
    conflicts_msg = f"Conflicting {attribute_name} relations"

    for row in result:
        row = dict(row)
        # Log conflicting relations
        if (row.get("row_number") or 0) > 1:

            if conflicts < _MAX_RELATION_CONFLICTS:
                row['volgnummer'] = row.get('src_volgnummer')
                issue = Issue(QA_CHECK.Unique_destination, row, 'src_id', 'bronwaarde')
                issue.attribute = attribute_name
                log_issue(logger, QA_LEVEL.WARNING, issue)
            conflicts += 1

    if conflicts > _MAX_RELATION_CONFLICTS:
        logger.data_warning(f"{conflicts_msg}: {conflicts} found, "
                            f"{min(conflicts, _MAX_RELATION_CONFLICTS)} reported")
