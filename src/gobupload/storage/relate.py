"""
Module contains the storage related logic for GOB Relations

"""
from gobupload.storage.handler import GOBStorageHandler

from gobcore.model import GOBModel
from gobcore.sources import GOBSources


def _get_match(field, spec):
    """
    For single references the match is on equality
    For multi references the match is on contains

    :param field:
    :param spec:
    :return: the match expression
    """
    field_name = spec['field_name']
    if field['type'] == "GOB.ManyReference":
        # destination field value in source bronwaarden
        return f"ANY(ARRAY(SELECT x->>'bronwaarde' FROM jsonb_array_elements(src.{field_name}) as x))"
    else:
        # destination field value = source bronwaarde
        return f"src.{field_name}->>'bronwaarde'"


def _get_data(query):
    """
    Execute the query and return the result as a list of dictionaries

    :param query:
    :return:
    """
    storage = GOBStorageHandler()
    engine = storage.engine
    data = engine.execute(query).fetchall()
    return [dict(row) for row in data]


def _get_fields(has_states):
    """
    Return the fields that have to be selected. Include state fields if the entity has states

    :param has_states:
    :return:
    """
    BASE_FIELDS = ["_id"]
    STATE_FIELDS = ["volgnummer", "begin_geldigheid", "eind_geldigheid"]

    fields = list(BASE_FIELDS)
    if has_states:
        fields.extend(STATE_FIELDS)
    return fields


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
    dst_match_fields = [spec['destination_attribute'] for spec in relation_specs]

    # Define the join of source and destination, src:bronwaarde = dst:field:value
    join_on = ([f"(src._application = '{spec['source']}' AND " +
                f"dst.{spec['destination_attribute']} = {_get_match(src_field, spec)})" for spec in relation_specs])

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
            "(dst.begin_geldigheid < src.eind_geldigheid OR src.eind_geldigheid is NULL)",
            "(dst.eind_geldigheid > src.begin_geldigheid OR dst.eind_geldigheid is NULL)"
        ])

    # Main order is on src id
    order_by = ["src._id"]
    if src_has_states:
        # then on source volgnummer and begin geldigheid
        order_by.extend(["src.volgnummer", "src.begin_geldigheid"])
    if dst_has_states:
        # then on destination begin and eind geldigheid
        order_by.extend(["dst.begin_geldigheid, dst.eind_geldigheid"])

    query = f"""
SELECT
    {comma_join.join([f'src.{field} AS src_{field}' for field in src_fields])},
    {comma_join.join([f'dst.{field} AS dst_{field}' for field in dst_fields])}
FROM {src_table_name} AS src
LEFT OUTER JOIN (
SELECT
    {comma_join.join([f'{field}' for field in dst_fields])},
    {comma_join.join([f'{field}' for field in dst_match_fields])}
FROM {dst_table_name}) AS dst
ON
    {and_join.join(join_on)}
ORDER BY
    {', '.join(order_by)}
"""

    # Example result
    # 1
    # A06.1 : 2006-06-12 - 2007-06-12 : ['B1']
    # A06.1 : 2007-06-12 - 2011-12-28 : ['B1', 'A1']
    # 2
    # A06.2 : 2011-12-28 - 2012-01-01 : ['B1', 'A2']
    # A06.2 : 2012-01-01 - 2014-01-01 : ['A2']
    # A06.2 : 2014-01-01 - 2015-01-01 : []
    # 3
    # A06.3 : 2015-01-01 - 2017-01-01 : ['A3', 'B2']
    # A06.3 : 2017-01-01 - None : ['B2']

    return _get_data(query)
