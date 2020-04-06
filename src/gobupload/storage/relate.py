"""
Module contains the storage related logic for GOB Relations

"""
import datetime
import json

from gobupload.storage.handler import GOBStorageHandler

from gobcore.logging.logger import logger
from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobcore.model.relations import get_relation_name
from gobcore.sources import GOBSources
from gobcore.quality.issue import QA_CHECK, QA_LEVEL, Issue, log_issue

from gobupload.relate.exceptions import RelateException
from gobupload.relate.table.update_table import RelationTableRelater
from gobupload.storage.execute import _execute, _execute_multiple

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


def _get_bronwaarde(field_name, field_type):
    """
    Get the bronwaarde for the relation

    :param field_name:
    :param field_type:
    :return:
    """
    if field_type == "GOB.ManyReference":
        # bronwaarde is an array
        return f"ARRAY(SELECT x->>'bronwaarde' FROM jsonb_array_elements(src.{field_name}) as x)"
    elif field_type == "GOB.Reference":
        # bronwaarde is a value
        return f"src.{field_name}->>'bronwaarde'"


def _get_match(field, spec):
    """
    For single references the match is on equality
    For multi references the match is on contains

    :param field:
    :param spec:
    :return: the match expression
    """
    bronwaarde = _get_bronwaarde(spec['field_name'], field['type'])
    if field['type'] == "GOB.ManyReference":
        # destination field value in source bronwaarden
        return f"ANY({bronwaarde})"
    elif field['type'] == "GOB.Reference":
        # destination field value = source bronwaarde
        return bronwaarde


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


def _get_fields(has_states):
    """
    Return the fields that have to be selected. Include state fields if the entity has states

    :param has_states:
    :return:
    """
    # Functional identification
    BASE_FIELDS = [FIELD.DATE_DELETED, FIELD.SOURCE, FIELD.ID]
    # State fields for collections with states
    STATE_FIELDS = [FIELD.SEQNR, FIELD.START_VALIDITY, FIELD.END_VALIDITY]

    fields = list(BASE_FIELDS)
    if has_states:
        fields.extend(STATE_FIELDS)
    return fields


def get_last_change(catalog_name, collection_name):
    """
    Gets the eventid of the most recent change for the given catalog and collection

    :param catalog_name:
    :param collection_name:
    :return:
    """
    # Using MAX(eventid) doesn't use the available indexes correctly resulting in a slow query
    query = f"""
SELECT eventid
FROM   events
WHERE  catalogue = '{catalog_name}' AND
       entity = '{collection_name}' AND
       action != 'CONFIRM'
ORDER BY eventid DESC
LIMIT 1
"""
    last_change = _execute(query).scalar()
    return 0 if last_change is None else last_change


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


class RelationUpdater:

    # Execute updates every update interval queries
    UPDATE_INTERVAL = 1000

    def __init__(self, catalog_name, collection_name):
        """
        Initialize an updater for the given catalog and collection

        :param catalog_name:
        :param collection_name:
        """
        model = GOBModel()
        self.table_name = model.get_table_name(catalog_name, collection_name)
        self.queries = []

    def update(self, field_name, row):
        """
        Create an update query for the given arguments.
        Add the query to the list of queries
        If the number of queries in the list of queries exceeds the update interval execute the queries

        :param field_name:
        :param row:
        :return:
        """
        query = f"""
UPDATE {self.table_name}
SET    {field_name} = $quotedString${json.dumps(row[field_name])}$quotedString$
WHERE  {FIELD.GOBID} = {row[FIELD.GOBID]}
"""
        self.queries.append(query)
        if len(self.queries) >= RelationUpdater.UPDATE_INTERVAL:
            self.completed()

    def completed(self):
        """
        Execute a list of queries and reinitialize the list of queries

        :return:
        """
        if self.queries:
            _execute_multiple(self.queries)
        self.queries = []


def _geo_resolve(spec, query_type):
    """
    Resolve the join or where part of a geometric query

    :param spec: {'source_attribute', 'destination_attribute'}
    :param query_type: 'join' or 'where' to specify the part of the query to be resolved
    :return: the where or join query string part
    """
    src_geo = f"src.{spec['source_attribute']}"
    dst_geo = f"dst.{spec['destination_attribute']}"
    if query_type == JOIN:
        # In the future this part might depend on the geometric types of the geometries
        # Currently only surface lies in surface is resolved (eg ligt_in_buurt)
        resolvers = {
            LIES_IN: f"ST_Contains({dst_geo}::geometry, ST_PointOnSurface({src_geo}::geometry))"
            # for points: f"ST_Contains({dst_geo}::geometry, {src_geo}::geometry)"
        }
        return resolvers.get(spec["method"])
    elif query_type == WHERE:
        # Only take valid geometries into account
        return f"ST_IsValid({src_geo}) AND ST_IsValid({dst_geo})"


def _equals_resolve(spec, src_field, query_type):
    """
    Resolve the join or where part of a 'equals' query (eg bronwaarde == code)

    :param spec: {'destination_attribute'}
    :param src_field:
    :param query_type: 'join' or 'where' to specify the part of the query to be resolved
    :return: the where or join query string part
    """
    if query_type == JOIN:
        return f"dst.{spec['destination_attribute']} = {_get_match(src_field, spec)}"
    elif query_type == WHERE:
        return f"{_get_bronwaarde(spec['field_name'], src_field['type'])} IS NOT NULL"


def _resolve_match(spec, src_field, query_type):
    """
    Resolve the join or where part of a relation query

    :param spec: {'destination_attribute'}
    :param src_field:
    :param query_type: 'join' or 'where' to specify the part of the query to be resolved
    :return: the where or join query string part
    """
    assert query_type in [JOIN, WHERE], f"Error: unknown query part type {query_type}"
    assert spec["method"] in [EQUALS, LIES_IN], f"Error: unknown match type {spec['method']}"

    if spec["method"] == EQUALS:
        return _equals_resolve(spec, src_field, query_type)
    elif spec["method"] == LIES_IN:  # geometric
        return _geo_resolve(spec, query_type)


def _get_select_from_join(dst_fields, dst_match_fields):
    """
    Get the fiels to retrieve from the main part of the relation query

    :param dst_fields:
    :param dst_match_fields:
    :return:
    """
    select_from_join = [f'{field}' for field in dst_fields + dst_match_fields]
    return select_from_join


def _get_select_from(dst_fields, dst_match_fields, src_fields, src_match_fields):
    """
    Get the fields to retrieve from the destination part of the relation query

    :param dst_fields:
    :param dst_match_fields:
    :param src_fields:
    :param src_match_fields:
    :return:
    """
    select_from = [f'src.{field} AS src_{field}' for field in src_fields + src_match_fields] + \
                  [f'dst.{field} AS dst_{field}' for field in dst_fields] + \
                  [f'dst.{field} AS {DST_MATCH_PREFIX}{field}' for field in dst_match_fields]
    return select_from


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
                data = {k.replace('_', ' '): v for k, v in data.items() if v is not None}
                issue = Issue(check, data, 'id', 'bronwaarde')
                issue.attribute = attr
                log_issue(logger, QA_LEVEL.WARNING, issue)
        else:
            # Count historic warnings
            historic_count += 1

    items_name = f"{attr} {check['msg']}"
    if count > max_warnings:
        logger.warning(f"{items_name}: {count} actual errors, reported first {max_warnings} only")
    if historic_count > 0:
        logger.info(f"{items_name}: {historic_count} historical errors")


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
    # Get the source catalog, collection and field for the given names
    model = GOBModel()
    src_collection = model.get_collection(src_catalog_name, src_collection_name)
    src_table_name = model.get_table_name(src_catalog_name, src_collection_name)
    src_field = src_collection['all_fields'].get(src_field_name)
    src_has_states = model.has_states(src_catalog_name, src_collection_name)

    is_many = src_field['type'] == "GOB.ManyReference"

    main_select = ["_id as id", f"{src_field_name} ->> 'bronwaarde' as bronwaarde"]
    select = ["_id", f"{src_field_name} ->> 'bronwaarde'"]
    if src_has_states:
        state_select = ["volgnummer", "begin_geldigheid", "eind_geldigheid"]
        select.extend(state_select)
        main_select.extend(state_select)
    select = ",\n    ".join(select)
    main_select = ",\n    ".join(main_select)

    name = f"{src_collection_name} {src_field_name}"

    src = f"""
(
SELECT
    {select},
    _date_deleted,
    jsonb_array_elements({src_field_name}) as {src_field_name}
FROM
    {src_table_name}
) AS src
""" if is_many else src_table_name

    # Select all relations without bronwaarde
    #
    # ->> 'bronwaarde' IS NULL
    # is True for all json fields (including empty ones) that have no bronwaarde, or a null value for bronwaarde
    #
    bronwaarden = f"""
SELECT
    {main_select}
FROM
    {src}
WHERE
    _date_deleted IS NULL AND
    {src_field_name} ->> 'bronwaarde' IS NULL
GROUP BY
    {select}
"""
    _query_missing(bronwaarden, QA_CHECK.Sourcevalue_exists, name)

    dangling = f"""
SELECT
    {main_select}
FROM
    {src}
WHERE
    _date_deleted IS NULL AND
    {src_field_name} ->> 'bronwaarde' IS NOT NULL AND
    {src_field_name} ->> 'id' IS NULL
GROUP BY
    {select}
"""
    _query_missing(dangling, QA_CHECK.Reference_exists, name)


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
    updater = RelationTableRelater(catalog_name, collection_name, attribute_name)
    query = updater.get_conflicts_query()

    result = _execute(query, stream=True, max_row_buffer=25000)

    conflicts = 0
    conflicts_msg = f"Conflicting {attribute_name} relations"

    for row in result:
        row = dict(row)
        # Log conflicting relations
        if (row.get("row_number") or 0) > 1:
            data = {
                f"src{FIELD.ID}": row.get(f"src{FIELD.ID}")
            }
            data.update({f"src_{FIELD.SEQNR}": row.get(f"src_{FIELD.SEQNR}")}
                        if updater.src_has_states else {})

            data.update({
                "conflict": {
                    "id": row.get(f"dst{FIELD.ID}"),
                    "bronwaarde": row.get(FIELD.SOURCE_VALUE),
                }
            })

            data["conflict"].update({f"{FIELD.SEQNR}": row.get(f"dst_{FIELD.SEQNR}")}
                                    if updater.dst_has_states else {})

            if conflicts < _MAX_RELATION_CONFLICTS:
                logger.warning(conflicts_msg, {
                    'id': conflicts_msg,
                    'data': data
                })
            conflicts += 1

    if conflicts > _MAX_RELATION_CONFLICTS:
        logger.warning(f"{conflicts_msg}: {conflicts} found, "
                       f"{min(conflicts, _MAX_RELATION_CONFLICTS)} reported")


def _update_match(spec, field, query_type, is_very_many=False):
    if spec['method'] == EQUALS:
        if query_type == JOIN:

            return f"dst.{spec['destination_attribute']} = rel.bronwaarde" \
                if is_very_many else f"dst.{spec['destination_attribute']} = {field} ->> 'bronwaarde'"
        else:
            return f"rel.bronwaarde IS NOT NULL" \
                if is_very_many else f"{field} IS NOT NULL AND {field} ->> 'bronwaarde' IS NOT NULL"
    else:
        return _geo_resolve(spec, query_type)


def update_relations(src_catalog_name, src_collection_name, src_field_name):
    """
    Compose a database query to get all relation data for the given catalog, collection and field
    :param src_catalog_name:
    :param src_collection_name:
    :param src_field_name:
    :return: a list of relation objects
    """

    # Get the source catalog, collection and field for the given names
    model = GOBModel()
    src_collection = model.get_collection(src_catalog_name, src_collection_name)
    src_field = src_collection['all_fields'].get(src_field_name)
    src_table_name = model.get_table_name(src_catalog_name, src_collection_name)

    is_many = src_field['type'] == "GOB.ManyReference"
    is_very_many = src_field['type'] == "GOB.VeryManyReference"

    if is_very_many:
        return 0

    # Get the relations for the given catalog, collection and field names
    sources = GOBSources()
    relation_specs = sources.get_field_relations(src_catalog_name, src_collection_name, src_field_name)
    if not relation_specs:
        raise RelateException("Missing relation specification for " +
                              f"{src_catalog_name} {src_collection_name} {src_field_name} " +
                              "(sources.get_field_relations)")

    # Get the destination catalog and collection names
    dst_catalog_name, dst_collection_name = src_field['ref'].split(':')
    dst_table_name = model.get_table_name(dst_catalog_name, dst_collection_name)

    # Check if source or destination has states (volgnummer, begin_geldigheid, eind_geldigheid)
    src_has_states = model.has_states(src_catalog_name, src_collection_name)
    dst_has_states = model.has_states(dst_catalog_name, dst_collection_name)

    # And get the source and destination fields to select
    src_fields = _get_fields(src_has_states)
    dst_fields = _get_fields(dst_has_states)

    # Get the fields that are required to match upon (multiple may exist, one per application)
    # Use dict.fromkeys to preserve field order
    src_match_fields = list(dict.fromkeys([spec['source_attribute'] for spec in relation_specs
                                           if spec.get('source_attribute') is not None]))
    dst_match_fields = list(dict.fromkeys([spec['destination_attribute'] for spec in relation_specs]))

    matches = [f"WHEN src._application = '{spec['source']}' THEN dst.{spec['destination_attribute']}"
               for spec in relation_specs]
    methods = [f"WHEN src._application = '{spec['source']}' THEN '{spec['method']}'"
               for spec in relation_specs]

    # Build a properly formatted select statement
    space_join = ' \n    '
    comma_join = ',\n    '
    and_join = ' AND\n    '
    or_join = ' OR\n    '

    # Define the join of source and destination, src:bronwaarde = dst:field:value
    json_join_alias = 'json_arr_elm'

    join_relation = json_join_alias if is_many else f"src.{src_field_name}"
    # Only get relations when bronwaarde is filled
    has_bronwaarde = ([f"(src.{FIELD.APPLICATION} = '{spec['source']}' AND " +
                       f"{_update_match(spec, join_relation, WHERE, is_very_many)})" for spec in relation_specs])

    join_on = _relate_update_join_on(src_has_states, dst_has_states, relation_specs, join_relation, is_very_many)

    order_by = _relate_update_order_by(src_has_states, dst_has_states)

    select_from = _get_select_from(dst_fields, dst_match_fields, src_fields, src_match_fields)
    select_from_join = _get_select_from_join(dst_fields, dst_match_fields)

    not_deleted = f"(src.{FIELD.DATE_DELETED} IS NULL AND dst.{FIELD.DATE_DELETED} IS NULL)"

    src_identification = "src__id, src_volgnummer" if src_has_states else "src__id"
    dst_identification = "dst__id, max(dst_volgnummer) dst_volgnummer" if dst_has_states else "dst__id"

    select_many_start = f"""
    SELECT
        {src_identification},
        to_jsonb(array_to_json(array_agg(src_matchcolumn))) src_matchcolumn,
        to_jsonb(array_to_json(array_agg({src_field_name}_updated_elm))) {src_field_name}_updated
    FROM (
""" if is_many else ""

    select_many_end = f"""
    ) last_dsts
    GROUP BY
        {src_identification}
""" if is_many else ""

    join_many = f"""
JOIN jsonb_array_elements(src.{src_field_name}) AS {json_join_alias} ON TRUE
""" if is_many else ""

    updated = "updated_elm" if is_many else "updated"
    src_value = "json_arr_elm" if is_many else f"src.{src_field_name}"

    jsonb_set_arg = f"""
jsonb_set(src_matchcolumn, '{{volgnummer}}', COALESCE(to_jsonb(max(dst_volgnummer)), 'null'::JSONB))
""" if dst_has_states else 'src_matchcolumn'

    matchcolumn_value = "rel.bronwaarde" if is_very_many else f"{src_value}"
    matchcolumn_value += " AS src_matchcolumn"

    relation_columns = ['dst_id'] if is_very_many else []
    if is_very_many and dst_has_states:
        relation_columns.append('dst_volgnummer')

    relation_select = f"{comma_join.join([f'rel_{field}' for field in relation_columns])}," if is_very_many else ""
    relation_select_from = f"{comma_join.join([f'rel.{field} AS rel_{field}' for field in relation_columns])}," \
                           if is_very_many else ""

    updated_column = f"""
{comma_join.join([f'{DST_MATCH_PREFIX}{field}' for field in dst_match_fields])}
""" if is_very_many else f"""
jsonb_set({jsonb_set_arg}, '{{id}}', COALESCE(to_jsonb(dst__id::TEXT), 'null'::JSONB))
    {src_field_name}_{updated}
"""

    # For VeryManyReferences join the relation table
    relation_join_on = _relate_update_relation_table_join_on(src_has_states)
    relation_table = "rel_" + get_relation_name(model, src_catalog_name, src_collection_name, src_field_name)
    relation_join = f"""
LEFT JOIN
    {relation_table} rel
ON
    {and_join.join(relation_join_on)}
    """ if is_very_many else ""

    relation_group_columns = [f'{DST_MATCH_PREFIX}{field}' for field in dst_match_fields] + \
                             [f'rel_{field}' for field in relation_columns]
    relation_group_by = f", {comma_join.join(relation_group_columns)}" \
        if is_very_many else ""

    # Use IS DISTINCT FROM because comparing null values will result in unkown in postgres
    where_clause = f"""rel_dst_id IS DISTINCT FROM dst__id""" \
                   if is_very_many else f"src_matchcolumn != {src_field_name}_updated"
    where_clause += f""" AND rel_dst_volgnummer IS DISTINCT FROM dst_volgnummer""" \
                    if is_very_many and dst_has_states else ""

    new_values = f"""
    SELECT * FROM (
    {select_many_start}
        SELECT --update specs
            {src_identification},
            {dst_identification},
            src_matchcolumn,
            {relation_select}
            {updated_column}
        FROM ( --relations
            SELECT
                CASE {space_join.join(methods)} END AS method,
                CASE {space_join.join(matches)} END AS match,
                {matchcolumn_value},
                {relation_select_from}
                {comma_join.join(select_from)}
            FROM
                {src_table_name} AS src
            {join_many}
            {relation_join}
            LEFT OUTER JOIN (
                SELECT
                    {comma_join.join(select_from_join)}
                FROM
                    {dst_table_name}) AS dst
                ON
                    {and_join.join(join_on)}
                WHERE
                    {or_join.join(has_bronwaarde)} AND {not_deleted}
                ORDER BY
                    {', '.join(order_by)}
        ) relations
        GROUP BY
            {src_identification},
            src_matchcolumn,
            dst__id
            {relation_group_by}
    {select_many_end}
    ) _outer
    WHERE --only select relations that have changed
        {where_clause}
"""

    # Update the source tabel
    updates = _do_relate_update(new_values, src_field_name, src_has_states, dst_has_states, src_table_name, False)

    return updates


def _relate_update_order_by(src_has_states, dst_has_states):
    # Main order is on src id
    order_by = [f"src.{FIELD.SOURCE}", f"src.{FIELD.ID}"]
    if src_has_states:
        # then on source volgnummer and begin geldigheid
        order_by.extend([f"src.{FIELD.SEQNR}", f"src.{FIELD.START_VALIDITY}"])
    if dst_has_states:
        # then on destination begin and eind geldigheid
        order_by.extend([f"dst.{FIELD.START_VALIDITY}", f"dst.{FIELD.END_VALIDITY}"])
    return order_by


def _relate_update_join_on(src_has_states, dst_has_states, relation_specs, join_relation, is_very_many=False):
    or_join = ' OR\n    '

    join_on = ([f"(src.{FIELD.APPLICATION} = '{spec['source']}' AND " +
                f"{_update_match(spec, join_relation, JOIN, is_very_many)})" for spec in relation_specs])
    # If more matches have been defined that catch any of the matches
    if len(join_on) > 1:
        join_on = [f"({or_join.join(join_on)})"]
    # If both collections have states then join with corresponding geldigheid intervals
    if src_has_states and dst_has_states:
        join_on.extend([
            f"(dst.{FIELD.START_VALIDITY} < src.{FIELD.END_VALIDITY} OR src.{FIELD.END_VALIDITY} IS NULL)",
            f"(dst.{FIELD.END_VALIDITY} >= src.{FIELD.END_VALIDITY} OR dst.{FIELD.END_VALIDITY} IS NULL)"
        ])
    elif dst_has_states:
        # If only destination has states, get the destination that is valid until forever
        join_on.extend([
            f"(dst.{FIELD.END_VALIDITY} IS NULL OR dst.{FIELD.END_VALIDITY} > NOW())"
        ])
    return join_on


def _relate_update_relation_table_join_on(src_has_states):
    join_on = [f"src.{FIELD.ID} = rel.src{FIELD.ID}"]
    if src_has_states:
        join_on.append(f"src.{FIELD.SEQNR} = rel.{FIELD.SEQNR}")
    return join_on


def _do_relate_update(new_values, src_field_name, src_has_states, dst_has_states,
                      update_table_name, is_very_many=False):
    """
    Update relations in chunks of 100,000 relations

    :param new_values: query to get any new updates
    :param src_field_name: name of the source field
    :param src_has_states: true if source has states (volgnummer)
    :param src_table_name: name of the source table
    :param is_very_many: boolean to see if it's a VeryManyReference
    :return: total number of updates executed
    """
    # Deteremine max number of upates, stop when this number of updates has been done or when no updates are left
    count_query = f"""
        SELECT
            count(*) AS count
        FROM (
            {new_values}
        ) new_values
    """
    count_data = _get_data(count_query)
    count = next(count_data)['count']
    offset = 0
    logger.info(f"{src_field_name}, max {count} entities to update")

    CHUNK_SIZE = 50000
    chunk = 0
    updates = 0
    while count > 0:
        logger.info(f"{src_field_name}, off {offset}, load {(chunk * CHUNK_SIZE):,} - {((chunk + 1) * CHUNK_SIZE):,}")

        if is_very_many:
            query = f"""
            UPDATE
                {update_table_name} src
            SET
                dst{FIELD.ID} = new_values.dst_{FIELD.ID}
                {', dst_volgnummer = new_values.dst_volgnummer' if dst_has_states else ''}
            """

            match_on = ['new_values.src__id = src.src_id', 'new_values.src_matchcolumn = src.bronwaarde']
            if src_has_states:
                match_on.append('new_values.src_volgnummer = src.src_volgnummer')
        else:
            query = f"""
            UPDATE
                {update_table_name} src
            SET
                {src_field_name} = new_values.{src_field_name}_updated
            """

            match_on = ['new_values.src__id = src._id']
            if src_has_states:
                match_on.append('new_values.src_volgnummer = src.volgnummer')

        query += f"""
                FROM (
                    --Select from all changed relations
                    {new_values}
                    --Get changed relations in chunks
                    ORDER BY
                        src__id
                    LIMIT
                        {CHUNK_SIZE}
                    OFFSET
                        {offset}
                ) new_values
                WHERE
                    {' AND '.join(match_on)}
        """
        result = _execute(query)
        n_updates = _get_updated_row_count(result.rowcount, CHUNK_SIZE)
        chunk += 1
        updates += n_updates
        count -= CHUNK_SIZE
        offset += (CHUNK_SIZE - n_updates)
        if n_updates <= 0:
            # Stop when no updates are left
            break

    logger.info(f"{src_field_name}, processed {updates} updates")
    return updates


def _get_updated_row_count(row_count, chunk_size):
    # Raise an expection if row count is greater than CHUNK_SIZE
    # to prevent "OFFSET must not be negative" SQL error.
    if row_count > chunk_size:
        raise RelateException(f"Updated row count {row_count} is greater than CHUNK_SIZE {chunk_size}")
    return row_count
