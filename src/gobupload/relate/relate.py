"""
GOB Relations

GOB Relations are defined in GOB Model.
The exact nature of a relation depends on the application that delivers the data.
This can be found in GOB Sources

This module uses the information in GOB Model and GOB Sources to get the relation data.
In time this information can change.

GOB Compare will then evaluate the data and translate the data into events.

"""
import datetime

from gobupload.storage.relate import get_relations


# Relations can have missing begin and end dates.
# Begin-of-time and end-of-time are used to cope with this.
_BEGIN_OF_TIME = datetime.date.min
_END_OF_TIME = datetime.date.max


def _handle_state_relation(state, relation):
    """
    Process a state (src) with its relations

    :param state:
    :param relation:
    :return:
    """
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
    """
    Handle each state (src) and its corresponding relations and transform it in a sorted and closed
    sequence of timeslots

    :param state:
    :param relations:
    :return:
    """
    relations.sort(key=lambda r: (r["begin_geldigheid"] if r["begin_geldigheid"] else _BEGIN_OF_TIME,
                                  r["eind_geldigheid"] if r["eind_geldigheid"] else _END_OF_TIME))

    relation = {}
    results = []
    for row in relations:
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


def _get_src_id(row):
    """
    Get the unique source id for a given row
    If the source has states the volgnummer is included

    :param row:
    :return:
    """
    return f"{row['src__id']}.{row['src_volgnummer']}" if row.get('src_volgnummer') else row['src__id']


def _get_relation(begin_geldigheid, eind_geldigheid, dst):
    """
    Compose relation data for the given timeslot and destination

    :param begin_geldigheid:
    :param eind_geldigheid:
    :param dst:
    :return:
    """
    return {
        "begin_geldigheid": begin_geldigheid,
        "eind_geldigheid": eind_geldigheid,
        "dst": [dst] if dst else []
    }


def _close_state(state, relations, previous_end, results):
    """
    If the source has a longer lifetime than the destination add an empty relation at the end

    :param state:
    :param relations:
    :param previous_end:
    :param results:
    :return:
    """
    if relations:
        if previous_end != state["end"]:
            # Add an empty last relation
            relations.append(_get_relation(previous_end, state["end"], {}))
        results.extend(_handle_state(state, relations))


def _get_id(row, id, volgnummer):
    """
    The identification of a src or dst is its id and an optional volgnummer

    :param row:
    :param id:
    :param volgnummer:
    :return:
    """
    return {
        "id": row[id],
        "volgnummer": row.get(volgnummer)
    }


def _add_relations_before_dst_begin(src_begin, dst_begin, relations):
    """
    If the destination begins later than the source begins, add an empty relation

    :param src_begin:
    :param dst_begin:
    :param relations:
    :return: The new destination begin
    """
    if src_begin is None and dst_begin:
        relations.append(_get_relation(_BEGIN_OF_TIME, dst_begin, {}))

    if src_begin and dst_begin:
        # Compare begin of source and destination
        if src_begin < dst_begin:
            # Insert empty relation until begin of destination
            relations.append(_get_relation(src_begin, dst_begin, {}))
        # Adjust destination begin to be equal or after source begin
        dst_begin = max(src_begin, dst_begin)

    return dst_begin


def _handle_relations(rows):
    """
    The relation data that is retrieved from the database is transformed into relation data
    with timeslots that cover the complete lifetime of the source field.

    :param rows: database query results
    :return: array with relations ordered by timeslot
    """
    if not rows:
        print("Warning: No relations found")
        return []

    state = {}
    previous = {}
    relations = []
    results = []
    for row in rows:

        # Get the source specs
        src = _get_src_id(row)
        src_id = _get_id(row, 'src__id', 'src_volgnummer')
        src_begin = row.get("src_begin_geldigheid")
        src_end = row.get("src_eind_geldigheid")

        # Get the destination specs
        dst_id = _get_id(row, 'dst__id', 'dst_volgnummer')
        dst_begin = row.get("dst_begin_geldigheid", src_begin)
        dst_end = row.get("dst_eind_geldigheid", src_end)

        if src != previous.get("src"):
            # end any current state on change of source (id + volgnummer)
            _close_state(state, relations, previous.get("dst_end"), results)
            # start new state
            state = {
                "src": src,
                "src_id": src_id,
                "begin": src_begin,
                "end": src_end,
            }
            relations = []

        dst_begin = _add_relations_before_dst_begin(src_begin, dst_begin, relations)

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

    _close_state(state, relations, previous["dst_end"], results)

    return results


def relate(catalog_name, collection_name, field_name):
    """
    Get all relations for the given catalog, collection and field

    :param catalog_name:
    :param collection_name:
    :param field_name:
    :return: the relations for the given catalog, collection and field
    """
    relations = get_relations(catalog_name, collection_name, field_name)
    return _handle_relations(relations)
