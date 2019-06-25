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

from gobupload.relate.exceptions import RelateException


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


def _execute_multiple(queries):
    engine = GOBStorageHandler.get_engine()

    result = None
    with engine.connect() as connection:
        for query in queries:
            result = connection.execute(query)

    return result   # Return result of last execution


def _execute(query):
    return _execute_multiple([query])


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
    storage = GOBStorageHandler()
    engine = storage.engine
    data = engine.execute(query)
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
ORDER BY timestamp DESC
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


def _query_missing(query, items_name, max_warnings=50):
    """
    Query for anu missing attributes

    :param query: query to execute
    :param items_name: name of the missing attribute
    :return: None
    """
    count = {
        'current': 0,
        'historic': 0
    }
    for data in _get_data(query):
        period = 'current' if data.get('eind_geldigheid') is None else 'historic'
        if count[period] < max_warnings:
            msg = f"{period} {items_name}"
            logger.warning(msg, {
                'id': msg,
                'data': {k: v for k, v in data.items() if v is not None}
            })
        count[period] += 1
        if count[period] == max_warnings:
            logger.warning(f"Too many (>{max_warnings}) {items_name}")
        if count['current'] >= max_warnings and count['historic'] >= max_warnings:
            break


def check_relations(src_catalog_name, src_collection_name, src_field_name):
    """
    Check relations for any dangling relations

    Dangling can be because a relation exist without any bronwaarde
    or the bronmwaarde cannot be matched with any referenced entity

    :param src_catalog_name:
    :param src_collection_name:
    :param src_field_name:
    :return: None
    """
    # Get the source catalog, collection and field for the given names
    model = GOBModel()

    src_table_name = model.get_table_name(src_catalog_name, src_collection_name)
    rel_name = get_relation_name(model, src_catalog_name, src_collection_name, src_field_name)
    rel_table_name = f"rel_{rel_name}"

    src_has_states = model.has_states(src_catalog_name, src_collection_name)

    select = ["_id"]
    where = [f"src_id = {src_table_name}._id"]
    if src_has_states:
        select.extend(["volgnummer", "begin_geldigheid", "eind_geldigheid"])
        where.extend([f"src_volgnummer = {src_table_name}.volgnummer"])
    select = ",\n    ".join(select)
    where = " AND\n            ".join(where)

    # select all relations that do not have an entry in the relations table
    #
    # ->> 'bronwaarde' IS NOT NULL
    # is True for all json fields that have a non NULL value for bronwaarde
    #
    srcs_without_relations = f"""
SELECT
    {select}
FROM
    {src_table_name}
WHERE
    _date_deleted IS NULL AND
    {src_field_name} ->> 'bronwaarde' IS NOT NULL AND
    NOT EXISTS (
        SELECT
            1
        FROM
            {rel_table_name}
        WHERE
            _date_deleted IS NULL AND
            {where}
    )
"""
    _query_missing(srcs_without_relations, f"missing relations")

    # Select all relations that do not point to a destination
    relations_without_dst = f"""
SELECT
    src_id,
    src_volgnummer,
    begin_geldigheid,
    eind_geldigheid
FROM
    {rel_table_name}
WHERE
    _date_deleted IS NULL AND
    dst_id IS NULL
"""
    _query_missing(relations_without_dst, "dangling relations")

    # Select all relations without bronwaarde
    #
    # ->> 'bronwaarde' IS NULL
    # is True for all json fields (including empty ones) that have no bronwaarde, or a null value for bronwaarde
    #
    select = ["_id"]
    if src_has_states:
        select.extend(["volgnummer", "begin_geldigheid", "eind_geldigheid"])
    select = ",\n    ".join(select)
    bronwaarden = f"""
SELECT
    {select}
FROM
    {src_table_name}
WHERE
    _date_deleted IS NULL AND
    {src_field_name} ->> 'bronwaarde' IS NULL
"""
    _query_missing(bronwaarden, "missing bronwaarden")


def get_relations(src_catalog_name, src_collection_name, src_field_name):
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

    # Define the join of source and destination, src:bronwaarde = dst:field:value
    join_on = ([f"(src.{FIELD.APPLICATION} = '{spec['source']}' AND " +
                f"{_resolve_match(spec, src_field, JOIN)})" for spec in relation_specs])

    # Only get relations when bronwaarde is filled
    has_bronwaarde = ([f"(src.{FIELD.APPLICATION} = '{spec['source']}' AND " +
                       f"{_resolve_match(spec, src_field, WHERE)})" for spec in relation_specs])

    # Build a properly formatted select statement
    space_join = ' \n    '
    comma_join = ',\n    '
    and_join = ' AND\n    '
    or_join = ' OR\n    '

    # If more matches have been defined that catch any of the matches
    if len(join_on) > 1:
        join_on = [f"({or_join.join(join_on)})"]

    # If both collections have states then join with corresponding geldigheid intervals
    if src_has_states and dst_has_states:
        join_on.extend([
            f"(dst.{FIELD.START_VALIDITY} <= src.{FIELD.END_VALIDITY} OR src.{FIELD.END_VALIDITY} IS NULL)",
            f"(dst.{FIELD.END_VALIDITY} >= src.{FIELD.START_VALIDITY} OR dst.{FIELD.END_VALIDITY} IS NULL)"
        ])

    # Main order is on src id
    order_by = [f"src.{FIELD.SOURCE}", f"src.{FIELD.ID}"]
    if src_has_states:
        # then on source volgnummer and begin geldigheid
        order_by.extend([f"src.{FIELD.SEQNR}::int", f"src.{FIELD.START_VALIDITY}"])
    if dst_has_states:
        # then on destination begin and eind geldigheid
        order_by.extend([f"dst.{FIELD.START_VALIDITY}", f"dst.{FIELD.END_VALIDITY}"])

    select_from = _get_select_from(dst_fields, dst_match_fields, src_fields, src_match_fields)
    select_from_join = _get_select_from_join(dst_fields, dst_match_fields)

    not_deleted = f"(src.{FIELD.DATE_DELETED} IS NULL AND dst.{FIELD.DATE_DELETED} IS NULL)"

    query = f"""
SELECT
    CASE
    {space_join.join(methods)} END AS method,
    CASE
    {space_join.join(matches)} END AS match,
    {comma_join.join(select_from)}
FROM {src_table_name} AS src
LEFT OUTER JOIN (
SELECT
    {comma_join.join(select_from_join)}
FROM {dst_table_name}) AS dst
ON
    {and_join.join(join_on)}
WHERE
    {or_join.join(has_bronwaarde)} AND
    {not_deleted}
ORDER BY
    {', '.join(order_by)}
"""
    # Example result
    # {
    #     'method': 'equals',
    #     'match': 'BC27',
    #     'src__date_deleted': None,
    #     'src__source': 'AMSBI',
    #     'src__id': '11080002',
    #     'dst__date_deleted': None,
    #     'dst__source': 'AMSBI',
    #     'dst__id': '03630012100860',
    #     'dst_volgnummer': '1',
    #     'dst_begin_geldigheid': datetime.date(2006, 6, 12),
    #     'dst_eind_geldigheid': None,
    #     'dst_match_code': 'BC27'
    # }
    return _get_data(query), src_has_states, dst_has_states


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

    # Define the join of source and destination, src:bronwaarde = dst:field:value
    join_on = ([f"(src.{FIELD.APPLICATION} = '{spec['source']}' AND " +
                f"dst.{spec['destination_attribute']} = json_arr_elm->>'bronwaarde')" for spec in relation_specs])

    # Only get relations when bronwaarde is filled
    has_bronwaarde = ([f"(src.{FIELD.APPLICATION} = '{spec['source']}' AND " +
                       f"json_arr_elm->>'bronwaarde' IS NOT NULL)" for spec in relation_specs])

    # Build a properly formatted select statement
    space_join = ' \n    '
    comma_join = ',\n    '
    and_join = ' AND\n    '
    or_join = ' OR\n    '

    # If more matches have been defined that catch any of the matches
    if len(join_on) > 1:
        join_on = [f"({or_join.join(join_on)})"]

    # If both collections have states then join with corresponding geldigheid intervals
    if src_has_states and dst_has_states:
        join_on.extend([
            f"(dst.{FIELD.START_VALIDITY} <= src.{FIELD.END_VALIDITY} OR src.{FIELD.END_VALIDITY} IS NULL)",
            f"(dst.{FIELD.END_VALIDITY} >= src.{FIELD.START_VALIDITY} OR dst.{FIELD.END_VALIDITY} IS NULL)"
        ])

    # Main order is on src id
    order_by = [f"src.{FIELD.SOURCE}", f"src.{FIELD.ID}"]
    if src_has_states:
        # then on source volgnummer and begin geldigheid
        order_by.extend([f"src.{FIELD.SEQNR}::int", f"src.{FIELD.START_VALIDITY}"])
    if dst_has_states:
        # then on destination begin and eind geldigheid
        order_by.extend([f"dst.{FIELD.START_VALIDITY}", f"dst.{FIELD.END_VALIDITY}"])

    select_from = _get_select_from(dst_fields, dst_match_fields, src_fields, src_match_fields)
    select_from_join = _get_select_from_join(dst_fields, dst_match_fields)

    not_deleted = f"(src.{FIELD.DATE_DELETED} IS NULL AND dst.{FIELD.DATE_DELETED} IS NULL)"

    is_many = src_field['type'] == "GOB.ManyReference"

    src_identification = "src__id, src_volgnummer" if src_has_states else "src__id"
    dst_identification = "dst__id, max(dst_volgnummer) dst_volgnummer" if dst_has_states else "dst__id"

    select_many = f"""
    SELECT
        {src_identification},
        array_to_json(array_agg({src_field_name}_updated_elm)) {src_field_name}_updated
    FROM (
""" if is_many else ""

    group_many = f"""
    ) last_dsts
    GROUP BY {src_identification}
""" if is_many else ""

    join_many = f"""
JOIN jsonb_array_elements(src.{src_field_name}) AS json_arr_elm ON TRUE
""" if is_many else ""

    updated = "updated_elm" if is_many else "updated"
    src_value = "json_arr_elm" if is_many else f"src.{src_field_name}"

    query = f"""
--UPDATE
--    {src_table_name} src
--SET
--    {src_field_name} = new_vals.{src_field_name}_updated
SELECT
    new_vals.{src_field_name}_updated
FROM (
    {select_many}
        SELECT
            {src_identification},
            src_matchcolumn,
            {dst_identification},
            jsonb_set(src_matchcolumn, '{{id}}', COALESCE(to_jsonb(dst__id::TEXT), 'null'::JSONB)) {src_field_name}_{updated}
        FROM (
--
SELECT
    CASE
    {space_join.join(methods)} END AS method,
    CASE
    {space_join.join(matches)} END AS match,
    {src_value} AS src_matchcolumn,
    {comma_join.join(select_from)}
FROM {src_table_name} AS src
{join_many}
LEFT OUTER JOIN (
SELECT
    {comma_join.join(select_from_join)}
FROM {dst_table_name}) AS dst
ON
    {and_join.join(join_on)}
WHERE
    {or_join.join(has_bronwaarde)} AND
    {not_deleted}
ORDER BY
    {', '.join(order_by)}
--
) relations
GROUP BY {src_identification}, src_matchcolumn, dst__id
{group_many}
) new_vals
--WHERE
--    new_vals.src__id = src._id {'AND new_vals.src_volgnummer = src.volgnummer' if src_has_states else ''};
"""
    print("Query", query)
    return [], src_has_states, dst_has_states
