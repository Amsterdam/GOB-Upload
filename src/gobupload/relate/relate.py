import datetime

from gobcore.model import GOBModel
from gobcore.sources import GOBSources

from gobupload.storage.handler import GOBStorageHandler


_BEGIN_OF_TIME = datetime.date.min
_END_OF_TIME = datetime.date.max


def _handle_state_relation(state, relation):
    print("STATE", state)
    print("RELATION", relation)
    print(f"{state['src_id']} {relation.get('begin_geldigheid')} - {relation.get('eind_geldigheid')} : {relation['dst']}")
    if relation.get('begin_geldigheid', 'begin') == relation.get('eind_geldigheid', 'eind'):
        return []
    else:
        if relation.get('begin_geldigheid') == _BEGIN_OF_TIME:
            relation['begin_geldigheid'] = None
        return [{
        "src_id": state["src_id"],
        "begin_geldigheid": relation.get('begin_geldigheid'),
        "eind_geldigheid": relation.get('eind_geldigheid'),
        "dst": relation['dst']
    }]


def _handle_state(state, relations):
    relations.sort(key=lambda r: (r["begin_geldigheid"] if r["begin_geldigheid"] else _BEGIN_OF_TIME,
                                  r["eind_geldigheid"] if r["eind_geldigheid"] else _END_OF_TIME))

    relation = {}
    results = []
    for row in relations:
        print("ROW", row)
        if relation.get('begin_geldigheid') == row.get('begin_geldigheid'):
            relation['dst'] = relation.get('dst', [])
            relation['dst'].extend(row['dst'])
            if relation.get('eind_geldigheid') == row.get('eind_geldigheid'):
                # Same state
                continue
            else:
                # End state
                results.extend(_handle_state_relation(state, relation))
                relation['dst'] = []
                # Adjust row
                row['begin_geldigheid'] = relation.get('eind_geldigheid')
        elif relation:
            # End state
            results.extend(_handle_state_relation(state, relation))
        relation = row

    if relation:
        results.extend(_handle_state_relation(state, relation))
    return results


def _get_id(row, id, volgnummer):
    return f"{row[id]}.{row[volgnummer]}" if row.get(volgnummer) else row[id]


def _get_src_id(row):
    return _get_id(row, 'src__id', 'src_volgnummer')


def _get_dst_id(row):
    return _get_id(row, 'dst__id', 'dst_volgnummer')


def _get_relation(begin_geldigheid, eind_geldigheid, dst):
    return {
        "begin_geldigheid": begin_geldigheid,
        "eind_geldigheid": eind_geldigheid,
        "dst": [dst] if dst else []
    }


def _get_match(field, spec):
    field_name = spec['field_name']
    if field['type'] == "GOB.ManyReference":
        # destination field value in source bronwaarden
        return f"ANY(ARRAY(SELECT x->>'bronwaarde' FROM jsonb_array_elements(src.{field_name}) as x))"
    else:
        # destination field value = source bronwaarde
        return f"src.{field_name}->>'bronwaarde'"


def _get_relations(src_catalog_name, src_collection_name, src_field_name):

    # Get the source catalog, collection and field for the given names
    model = GOBModel()
    src_catalog = model.get_catalog(src_catalog_name)
    src_collection = model.get_collection(src_catalog_name, src_collection_name)
    src_field = src_collection['all_fields'].get(src_field_name)
    src_table_name = model.get_table_name(src_catalog_name, src_collection_name)

    # Get the relations for the given catalog, collection and field names
    sources = GOBSources()
    relation_specs = sources.get_field_relation(src_catalog_name, src_collection_name, src_field_name)

    # Get the destination catalog and collection names
    dst_catalog_name, dst_collection_name = src_field['ref'].split(':')
    dst_table_name = model.get_table_name(dst_catalog_name, dst_collection_name)

    # Check if source or destination has states (volgnummer, begin_geldigheid, eind_geldigheid)
    src_has_states = model.has_states(src_catalog_name, src_collection_name)
    dst_has_states = model.has_states(dst_catalog_name, dst_collection_name)

    # And get the source and destination fields to select
    BASE_FIELDS = ["_id"]
    STATE_FIELDS = ["volgnummer", "begin_geldigheid", "eind_geldigheid"]

    src_fields = list(BASE_FIELDS)
    if src_has_states:
        src_fields.extend(STATE_FIELDS)

    dst_fields = list(BASE_FIELDS)
    if dst_has_states:
        dst_fields.extend(STATE_FIELDS)

    # Get the fields that are required to match upon (multiple may exist, one per application)
    dst_match_fields = [spec['destination_attribute'] for spec in relation_specs]

    # Define the join of source and destination, src:bronwaarde = dst:field:value
    join_on = ([f"(src._application = '{spec['source']}' AND " +
                f"dst.{spec['destination_attribute']} = {_get_match(src_field, spec)})" for spec in relation_specs])

    # If more matches have been defined that catch any of the matches
    if len(join_on) > 1:
        join_on = [f"({' OR '.join(join_on)})"]

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

    # Build a properly formatted select statement
    comma_join = ',\n    '
    and_join = ' AND\n    '
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

    print(query)
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

    # Execute the query and return the result as a list of dictionaries
    storage = GOBStorageHandler()
    engine = storage.engine
    relations = engine.execute(query).fetchall()
    relations = [dict(row) for row in relations]
    return relations


def _handle_relations(rows):
    if not rows:
        print("ERROR: No relations!")
        return []

    state = {}
    previous = {}
    relations = []
    results = []
    for row in rows:

        # Get the source specs
        src = _get_src_id(row)
        src_id = {
            "id": row['src__id'],
            "volgnummer": row.get("src_volgnummer")
        }
        src_begin = row.get("src_begin_geldigheid")
        src_end = row.get("src_eind_geldigheid")

        # Get the destination specs
        dst = _get_dst_id(row)
        dst_id = {
            "id": row['dst__id'],
            "volgnummer": row.get("dst_volgnummer")
        }
        dst_begin = row.get("dst_begin_geldigheid", src_begin)
        dst_end = row.get("dst_eind_geldigheid", src_end)

        if src != previous.get("src"):
            # end any current state
            if relations:
                if previous["dst_end"] != state["end"]:
                    # Add an empty last relation
                    relations.append(_get_relation(previous["dst_end"], state["end"], {}))
                results.extend(_handle_state(state, relations))
            # start new state
            state = {
                "src": src,
                "src_id": src_id,
                "begin": src_begin,
                "end": src_end,
            }
            relations = []

        if src_begin is None and dst_begin:
            relations.append(_get_relation(_BEGIN_OF_TIME, dst_begin, {}))

        if src_begin and dst_begin:
            # Compare begin of source and destination
            if src_begin < dst_begin:
                # Insert empty relation until begin of destination
                relations.append(_get_relation(src_begin, dst_begin, {}))
            # Adjust destination begin to be equal or after source begin
            dst_begin = max(src_begin, dst_begin)

        # Take the minimum eind_geldigheid of src_id and dst
        if dst_end is None:
            dst_end = src_end
        elif src_end is not None:
            dst_end = min(src_end, dst_end)

        relations.append(_get_relation(dst_begin, dst_end, dst_id))

        previous = {
            "src": src,
            "dst_end": dst_end
        }

    if relations:
        if previous["dst_end"] != state["end"]:
            # Add an empty last relation
            relations.append(_get_relation(previous["dst_end"], state["end"], {}))
        results.extend(_handle_state(state, relations))

    print(rows[0])
    return results


def relate(catalog_name, collection_name, field_name):
    relations = _get_relations(catalog_name, collection_name, field_name)
    _handle_relations(relations)
