"""Module contains the storage related logic for GOB Relations."""

import datetime

from gobcore.logging.logger import logger
from gobcore.model.metadata import FIELD
from gobcore.model.relations import get_relation_name
from gobcore.quality.issue import QA_CHECK, QA_LEVEL, Issue, log_issue
from gobcore.sources import GOBSources

from gobupload import gob_model
from gobupload.relate.update import Relater

from gobupload.storage.handler import GOBStorageHandler
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


def date_to_datetime(value):
    """Convert a date value to a datetime value.

    :param value: a date value
    :return: The corresponding datetime value
    """
    return datetime.datetime.combine(value, _START_OF_DAY)


def _get_date_origin_fields():
    """Get all date fieldnames.

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
    """Convert a database row to a dictionary.

    If any of the date fields has a datetime value, convert all date values to datetime values.

    :param row: A database row
    :return: A dictionary
    """
    result = dict(row)
    date_origin_fields = _get_date_origin_fields()

    has_datetimes = True in [
        isinstance(result.get(field), datetime.datetime) for field in date_origin_fields]
    if has_datetimes:
        for date_origin_field in date_origin_fields:
            value = result.get(date_origin_field)
            if value and not isinstance(value, datetime.datetime):
                result[date_origin_field] = date_to_datetime(value)

    return result


def _get_data(query):
    """Execute the query and return the result as a list of dictionaries.

    :param query:
    :return:
    """
    handler = GOBStorageHandler()
    with handler.get_session() as session:
        data = session.execute(query)
        for row in data:
            yield _convert_row(row)


def get_current_relations(catalog_name, collection_name, field_name):
    """Get the current relations as an iterable of dictionaries.

    Each relation is transformed into a dictionary.

    :param catalog_name:
    :param collection_name:
    :param field_name:
    :return: An iterable of dicts
    """
    table_name = gob_model.get_table_name(catalog_name, collection_name)

    collection = gob_model[catalog_name]['collections'][collection_name]
    field = collection['all_fields'][field_name]
    field_type = field['type']
    assert field_type in ["GOB.Reference", "GOB.ManyReference"], f"Error: unexpected field type '{field_type}'"

    select = [FIELD.GOBID, field_name, FIELD.SOURCE, FIELD.ID]
    order_by = [FIELD.SOURCE, FIELD.ID]
    if gob_model.has_states(catalog_name, collection_name):
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


def _query_missing(query, check, attr):
    """Query for any missing attributes.

    :param query: query to execute
    :param items_name: name of the missing attribute
    :return: None
    """
    historic_count = 0
    for data in _get_data(query):
        if data.get('eind_geldigheid') is None:
            # Report actual warnings
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
    if historic_count > 0:
        logger.data_info(f"{items_name}: {historic_count} historical errors")


def _get_relation_check_query(
        query_type,
        src_catalog_name, src_collection_name, src_field_name,
        filter_applications: list):
    assert query_type in ["dangling", "missing"], "Relation check query expects type to be dangling or missing"

    src_collection = gob_model[src_catalog_name]['collections'][src_collection_name]
    src_table_name = gob_model.get_table_name(src_catalog_name, src_collection_name)
    src_field = src_collection['all_fields'].get(src_field_name)
    src_has_states = gob_model.has_states(src_catalog_name, src_collection_name)

    is_many = src_field['type'] == "GOB.ManyReference"

    relation_table_name = "rel_" + get_relation_name(
        gob_model, src_catalog_name, src_collection_name, src_field_name)

    main_select = [f"src.{FIELD.ID} as id",
                   f"src.{FIELD.EXPIRATION_DATE}"]
    main_select.extend([f"rel.{FIELD.SOURCE_VALUE}"] if query_type == "dangling" else
                       [f"src.{src_field_name}->>'{FIELD.SOURCE_VALUE}' as {FIELD.SOURCE_VALUE}"])
    select = [FIELD.ID,
              FIELD.EXPIRATION_DATE,
              FIELD.DATE_DELETED,
              f"jsonb_array_elements({src_field_name}) as {src_field_name}"]

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
    {select}
FROM
    {src_table_name}
) AS src
""" if is_many and query_type == "missing" else f"{src_table_name} src"

    where = [f"src.{FIELD.DATE_DELETED} IS NULL"]

    # For missing relations check is bronwaarde is empty
    where.extend([f"{src_field_name}->>'bronwaarde' IS NULL"] if query_type == "missing" else [])

    # For dangling relations check if destination is empty
    where.extend(["rel.dst_id IS NULL", f"rel.{FIELD.DATE_DELETED} IS NULL"] if query_type == "dangling" else [])

    if filter_applications:
        ors = [f"src.{FIELD.APPLICATION} = '{application}'" for application in filter_applications]
        where.append(f"({' OR '.join(ors)})")

    where = " AND ".join(where)
    query = f"""
SELECT
    {main_select}
FROM
    {src}"""

    query += f"""
JOIN {relation_table_name} rel
ON
    {join_on}
""" if query_type == "dangling" else ""

    query += f"""
WHERE
    {where}
"""
    return query


def check_relations(src_catalog_name, src_collection_name, src_field_name):
    """Check relations for any dangling relations.

    Dangling can be because a relation exist without any bronwaarde
    or the bronwaarde cannot be matched with any referenced entity

    :param src_catalog_name:
    :param src_collection_name:
    :param src_field_name:
    :return: None
    """
    name = f"{src_collection_name} {src_field_name}"

    # Only include sources where not none_allowed
    sources = GOBSources(gob_model).get_field_relations(src_catalog_name, src_collection_name, src_field_name)
    check_sources = [source['source'] for source in sources if not source.get('none_allowed', False)]

    if not check_sources:
        logger.info(f"All sources for {src_catalog_name} {src_collection_name} {src_field_name} allow empty "
                    f"relations. Skipping check.")
        return

    # Only filter on sources when necessary (i.e. when there are multiple sources with different values for
    # none_allowed)
    check_sources = check_sources if len(sources) != len(check_sources) else None
    missing_query = _get_relation_check_query("missing", src_catalog_name, src_collection_name, src_field_name,
                                              check_sources)
    _query_missing(missing_query, QA_CHECK.Sourcevalue_exists, name)

    dangling_query = _get_relation_check_query("dangling", src_catalog_name, src_collection_name, src_field_name,
                                               check_sources)

    _query_missing(dangling_query, QA_CHECK.Reference_exists, name)


def check_very_many_relations(src_catalog_name, src_collection_name, src_field_name):
    """Check very many relations for any dangling relations.

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
    src_table_name = gob_model.get_table_name(src_catalog_name, src_collection_name)
    src_has_states = gob_model.has_states(src_catalog_name, src_collection_name)

    relation_table_name = "rel_" + get_relation_name(
        gob_model, src_catalog_name, src_collection_name, src_field_name)

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
    with Relater(catalog_name, collection_name, attribute_name) as updater:
        result = updater.get_conflicts()

        for row in result:
            row = dict(row)
            # Log conflicting relations
            if (row.get("row_number") or 0) > 1:
                row['volgnummer'] = row.get('src_volgnummer')
                issue = Issue(QA_CHECK.Unique_destination, row, 'src_id', 'bronwaarde')
                issue.attribute = attribute_name
                log_issue(logger, QA_LEVEL.WARNING, issue)
