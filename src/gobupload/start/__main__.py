import datetime

from gobcore.model import GOBModel
from gobcore.sources import GOBSources

from gobupload.storage.handler import GOBStorageHandler

storage = GOBStorageHandler()


def handle_state_relation(state, relation):

    print(f"{state['src']} {relation.get('begin_geldigheid')} - {relation.get('eind_geldigheid')} : {relation['dst']}")


def handle_state(state, relations):
    relations.sort(key=lambda r: (r["begin_geldigheid"],
                                  r["eind_geldigheid"] if r["eind_geldigheid"] else datetime.date.max))

    relation = {
        'dst': []
    }
    for row in relations:
        if relation.get('begin_geldigheid') == row.get('begin_geldigheid'):
            relation['dst'].extend(row['dst'])
            if relation.get('eind_geldigheid') == row.get('eind_geldigheid'):
                # Same state
                continue
            else:
                # End state
                handle_state_relation(state, relation)
                # Adjust row
                row['begin_geldigheid'] = relation.get('eind_geldigheid')
        elif relation:
            # End state
            handle_state_relation(state, relation)
        relation = row

    if relation:
        handle_state_relation(state, relation)


def get_src_id(row):
    id = row['src__id']
    if row.get('src_volgnummer'):
        id = f"{id}.{row['src_volgnummer']}"
    return id


def get_dst_id(row):
    id = row['dst__id']
    if row.get('dst_volgnummer'):
        id = f"{id}.{row['dst_volgnummer']}"
    return id


def get_relation(begin_geldigheid, eind_geldigheid, dst):
    return {
        "begin_geldigheid": begin_geldigheid,
        "eind_geldigheid": eind_geldigheid,
        "dst": [dst] if dst else []
    }


def get_match(field, spec):
    assert field['type'] in ["GOB.ManyReference", "GOB.Reference"]
    field_name = spec['field_name']
    if field['type'] == "GOB.ManyReference":
        return f"ANY(ARRAY(SELECT x->>'bronwaarde' FROM jsonb_array_elements(src.{field_name}) as x))"
    else:
        return f"src.{field_name}->>'bronwaarde'"


def get_relations(src_catalog_name, src_collection_name, src_field_name):

    model = GOBModel()
    src_catalog = model.get_catalog(src_catalog_name)
    src_collection = model.get_collection(src_catalog_name, src_collection_name)
    assert (src_catalog and src_collection), f"Invalid catalog '{src_catalog_name}' or collection '{src_collection_name}'"

    field = src_collection['all_fields'].get(src_field_name)
    assert field, f"No field specs found for '{src_catalog_name}.{src_collection_name}.{src_field_name}'"

    sources = GOBSources()
    relation_specs = sources.get_field_relation(src_catalog_name, src_collection_name, src_field_name)
    assert relation_specs, f"No relation specs found for '{src_catalog_name}.{src_collection_name}.{src_field_name}'"

    dst_catalog_name, dst_collection_name = field['ref'].split(':')

    src_has_states = model.has_states(src_catalog_name, src_collection_name)
    dst_has_states = model.has_states(dst_catalog_name, dst_collection_name)

    BASE_FIELDS = ["_id"]
    STATE_FIELDS = ["volgnummer", "begin_geldigheid", "eind_geldigheid"]

    src_fields = list(BASE_FIELDS)
    if src_has_states:
        src_fields.extend(STATE_FIELDS)

    dst_fields = list(BASE_FIELDS)
    if dst_has_states:
        dst_fields.extend(STATE_FIELDS)

    dst_match_fields = [spec['destination_attribute'] for spec in relation_specs]

    join_on = ([f"(src._application = '{spec['source']}' AND " +
                f"dst.{spec['destination_attribute']} = {get_match(field, spec)})" for spec in relation_specs])
    if len(join_on) > 1:
        join_on = [f"({' OR '.join(join_on)})"]

    if src_has_states and dst_has_states:
        join_on.extend([
            "(dst.begin_geldigheid < src.eind_geldigheid OR src.eind_geldigheid is NULL)",
            "(dst.eind_geldigheid > src.begin_geldigheid OR dst.eind_geldigheid is NULL)"
        ])

    order_by = ["src._id"]
    if src_has_states:
        order_by.extend(["src.volgnummer", "src.begin_geldigheid"])
    if dst_has_states:
        order_by.extend(["dst.begin_geldigheid, dst.eind_geldigheid"])

    src_table = model.get_table_name(src_catalog_name, src_collection_name)
    dst_table = model.get_table_name(dst_catalog_name, dst_collection_name)

    print(src_catalog_name, src_collection_name, src_field_name, dst_catalog_name, dst_collection_name)

    comma_join = ',\n    '
    and_join = ' AND\n    '
    query = f"""
SELECT
    {comma_join.join([f'src.{field} AS src_{field}' for field in src_fields])},
    {comma_join.join([f'dst.{field} AS dst_{field}' for field in dst_fields])}
FROM {src_table} AS src
LEFT OUTER JOIN (
SELECT
    {comma_join.join([f'{field}' for field in dst_fields])},
    {comma_join.join([f'{field}' for field in dst_match_fields])},
FROM {dst_table}) AS dst
ON
    {and_join.join(join_on)}
ORDER BY
    {', '.join(order_by)}
"""

    print(query)
    exit(0)
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
    engine = storage.engine
    relations = engine.execute(query).fetchall()
    relations = [dict(row) for row in relations]
    return relations


def handle_relations(rows):
    if not rows:
        print("ERROR: No relations!")
        return

    state = {}
    previous = {}
    relations = []
    for row in rows:

        src = get_src_id(row)
        src_begin = row.get("src_begin_geldigheid")
        src_end = row.get("src_eind_geldigheid")

        dst = get_dst_id(row)
        dst_begin = row.get("dst_begin_geldigheid")
        dst_end = row.get("dst_eind_geldigheid")

        if src == previous.get("src"):
            # Continue with same entity
            pass
        else:
            # end any current state
            if relations:
                if previous["dst_end"] != state["end"]:
                    # Add an empty last relation
                    relations.append(get_relation(previous["dst_end"], state["end"], None))
                handle_state(state, relations)
            # start new state
            state = {
                "src": src,
                "begin": src_begin,
                "end": src_end,
            }
            relations = []

        if src_begin and dst_begin:
            if src_begin < dst_begin:
                # Insert empty relation until begin of destination
                relations.append(get_relation(src_begin, dst_begin, None))
            dst_begin = max(src_begin, dst_begin)

        # Take the minimum eind_geldigheid of src_id and dst
        if dst_end is None:
            dst_end = src_end
        elif src_end is not None:
            dst_end = min(src_end, dst_end)

        relations.append(get_relation(dst_begin, dst_end, dst))

        previous = {
            "src": src,
            "dst_end": dst_end
        }

    if relations:
        handle_state(state, relations)


def main(catalog_name, collection_name, field_name):
    relations = get_relations(catalog_name, collection_name, field_name)
    handle_relations(relations)



# main("gebieden", "wijken", "ligt_in_stadsdeel") # has-states - has-states
# main("nap", "peilmerken", "ligt_in_bouwblok") # no-states - has-states
# has-states - no-states ??
main("meetbouten", "metingen", "hoort_bij_meetbout") # no-states - no-states

# many reference
# main("meetbouten", "metingen", "refereert_aan_referentiepunten")
