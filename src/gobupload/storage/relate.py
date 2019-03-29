"""
Module contains the storage related logic for GOB Relations

"""
import datetime
import json

from gobupload.storage.handler import GOBStorageHandler

from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
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
    storage = GOBStorageHandler()
    engine = storage.engine

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
    data = engine.execute(query).fetchall()
    return [_convert_row(row) for row in data]


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
    query = f"""
SELECT MAX(eventid)
FROM   events
WHERE  catalogue = '{catalog_name}' AND
       entity = '{collection_name}' AND
       action != 'CONFIRM'
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
SET    {field_name} = '{json.dumps(row[field_name])}'
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

    # Define the join of source and destination, src:bronwaarde = dst:field:value
    join_on = ([f"(src.{FIELD.APPLICATION} = '{spec['source']}' AND " +
                f"{_resolve_match(spec, src_field, JOIN)})" for spec in relation_specs])

    # Only get relations when bronwaarde is filled
    has_bronwaarde = ([f"(src.{FIELD.APPLICATION} = '{spec['source']}' AND " +
                       f"{_resolve_match(spec, src_field, WHERE)})" for spec in relation_specs])

    # Build a properly formatted select statement
    comma_join = ',\n    '
    and_join = ' AND\n    '
    or_join = ' OR\n    '

    # If more matches have been defined that catch any of the matches
    if len(join_on) > 1:
        join_on = [f"({or_join.join(join_on)})"]

    # If both collections have states then join with corresponding geldigheid intervals
    if src_has_states and dst_has_states:
        join_on.extend([
            f"(dst.{FIELD.START_VALIDITY} < src.{FIELD.END_VALIDITY} OR src.{FIELD.END_VALIDITY} IS NULL)",
            f"(dst.{FIELD.END_VALIDITY} > src.{FIELD.START_VALIDITY} OR dst.{FIELD.END_VALIDITY} IS NULL)"
        ])

    # Main order is on src id
    order_by = [f"src.{FIELD.SOURCE}", f"src.{FIELD.ID}"]
    if src_has_states:
        # then on source volgnummer and begin geldigheid
        order_by.extend([f"src.{FIELD.SEQNR}", f"src.{FIELD.START_VALIDITY}"])
    if dst_has_states:
        # then on destination begin and eind geldigheid
        order_by.extend([f"dst.{FIELD.START_VALIDITY}", f"dst.{FIELD.END_VALIDITY}"])

    select_from = _get_select_from(dst_fields, dst_match_fields, src_fields, src_match_fields)
    select_from_join = _get_select_from_join(dst_fields, dst_match_fields)

    not_deleted = f"(src.{FIELD.DATE_DELETED} IS NULL AND dst.{FIELD.DATE_DELETED} IS NULL)"

    query = f"""
SELECT
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
    #     'src__id': '10181000',
    #     'src__source': 'AMSBI',
    #     'dst__id': '03630012097126',
    #     'dst_volgnummer': '1',
    #     'dst__source': 'AMSBI',
    #     'dst_begin_geldigheid': datetime.date(2006, 6, 12),
    #     'dst_eind_geldigheid': None
    # }
    return _get_data(query), src_has_states, dst_has_states
