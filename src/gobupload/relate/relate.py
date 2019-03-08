"""
GOB Relations

GOB Relations are defined in GOB Model.
The exact nature of a relation depends on the application that delivers the data.
This can be found in GOB Sources

This module uses the information in GOB Model and GOB Sources to get the relation data.
In time this information can change.

GOB Compare will then evaluate the data and translate the data into events.

The result is organized as list of:
{
    'src': {
        'source': 'functional source',
        'id': 'functional id',
        'volgnummer': 'any value' or None
    },
    'begin_geldigheid': any date/datetime or None,
    'eind_geldigheid': any date/datetime or None,
    'dst': [
        {
            'source': 'functional source',
            'id': 'functional id',
            'volgnummer': 'any value' or None
        },
        ...
    ]
}


"""
import datetime

from gobcore.model.metadata import FIELD
from gobcore.logging.logger import logger

from gobupload.storage.relate import get_relations, date_to_datetime


# Relations can have missing begin and end dates.
# Begin-of-time and end-of-time are used to cope with this.
_BEGIN_OF_TIME = datetime.datetime.min
_END_OF_TIME = datetime.datetime.max


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
            "src": state["src_id"],
            "begin_geldigheid": relation.get('begin_geldigheid'),
            "eind_geldigheid": relation.get('eind_geldigheid'),
            "dst": relation['dst']
        }]


def _dates_sort(row):
    """
    Sort on start validity and then end validity

    Always sort on datetime, to allow for datetime - date comparisons
    :param row:
    :return: tuple(start-validity, end-validity)
    """
    start_validity = row["begin_geldigheid"]
    end_validity = row["eind_geldigheid"]
    if isinstance(start_validity, datetime.date):
        start_validity = date_to_datetime(start_validity)
    if isinstance(end_validity, datetime.date):
        end_validity = date_to_datetime(end_validity)
    return (start_validity if start_validity else _BEGIN_OF_TIME,
            end_validity if end_validity else _END_OF_TIME)


def _handle_state(state, relations):
    """
    Handle each state (src) and its corresponding relations and transform it in a sorted and closed
    sequence of timeslots

    :param state:
    :param relations:
    :return:
    """
    relations.sort(key=_dates_sort)

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
    src_id = f"src_{FIELD.ID}"
    src_volgnummer = f"src_{FIELD.SEQNR}"
    return f"{row[src_id]}.{row[src_volgnummer]}" if row.get(src_volgnummer) else row[src_id]


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


def _no_dst(source):
    return {
        "source": source,
        "id": None,
        "volgnummer": None
    }


def _get_id(row, source, id, volgnummer):
    """
    The identification of a src or dst is its id and an optional volgnummer

    :param row:
    :param id:
    :param volgnummer:
    :return:
    """
    return {
        "source": row[source],
        "id": row[id],
        "volgnummer": row.get(volgnummer)
    }


def _close_state(state, relations, previous, results):
    """
    If the source has a longer lifetime than the destination add an empty relation at the end

    :param state:
    :param relations:
    :param previous_end:
    :param results:
    :return:
    """
    if relations:
        dst_end = previous["dst_end"]
        no_dst = _no_dst(previous["dst_source"])
        if dst_end != state["end"]:
            # Add an empty last relation
            relations.append(_get_relation(dst_end, state["end"], no_dst))
        results.extend(_handle_state(state, relations))


def _add_relations_before_dst_begin(src_begin, dst_begin, dst_id, relations):
    """
    If the destination begins later than the source begins, add an empty relation

    :param src_begin:
    :param dst_begin:
    :param relations:
    :return: The new destination begin
    """
    no_dst = _no_dst(dst_id["source"])
    if src_begin is None and dst_begin:
        relations.append(_get_relation(_BEGIN_OF_TIME, dst_begin, no_dst))

    if src_begin and dst_begin:
        # Compare begin of source and destination
        if src_begin < dst_begin:
            # Insert empty relation until begin of destination
            relations.append(_get_relation(src_begin, dst_begin, no_dst))
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
    state = {}
    previous = {}
    relations = []
    results = []
    for row in rows:
        # Get the source specs
        src = _get_src_id(row)
        src_id = _get_id(row, f"src_{FIELD.SOURCE}", f"src_{FIELD.ID}", f"src_{FIELD.SEQNR}")
        src_begin = row.get(f"src_{FIELD.START_VALIDITY}")
        src_end = row.get(f"src_{FIELD.END_VALIDITY}")

        # Get the destination specs
        dst_id = _get_id(row, f"dst_{FIELD.SOURCE}", f"dst_{FIELD.ID}", f"dst_{FIELD.SEQNR}")
        dst_begin = row.get(f"dst_{FIELD.START_VALIDITY}", src_begin)
        dst_end = row.get(f"dst_{FIELD.END_VALIDITY}", src_end)

        if src != previous.get("src"):
            # end any current state on change of source (id + volgnummer)
            _close_state(state, relations, previous, results)
            # start new state
            state = {
                "src": src,
                "src_id": src_id,
                "begin": src_begin,
                "end": src_end,
            }
            relations = []
            # Initialize start date
            dst_begin = _add_relations_before_dst_begin(src_begin, dst_begin, dst_id, relations)

        # Take the minimum eind_geldigheid of src_id and dst
        if dst_end is None:
            dst_end = src_end
        elif src_end is not None:
            dst_end = min(src_end, dst_end)

        relations.append(_get_relation(dst_begin, dst_end, dst_id))

        previous = {
            "src": src,
            "src_id": src_id["id"],
            "src_begin": src_begin,
            "src_end": src_end,
            "dst_source": dst_id["source"],
            "dst_begin": dst_begin,
            "dst_end": dst_end
        }

    _close_state(state, relations, previous, results)

    # Example result
    # {
    #     'src': {'id': '26281033', 'source': 'AMSBI', 'volgnummer': None},
    #     'dst': [{'id': None, 'source': 'AMSBI', 'volgnummer': None}],
    #     'begin_geldigheid': None,
    #     'eind_geldigheid': datetime.date(2006, 6, 12)
    # }

    return results


def _remove_gaps(results):
    """
    Remove any erroneous results from the output

    Errors occur when start- and end dates are not consecutive.

    :param results:
    :return: results without gaps
    """
    previous = {}
    gaps = {}
    no_inconsistencies = []
    while results:
        result = results.pop(0)

        src_id = result["src"]["id"]
        src_volgnummer = result["src"]["volgnummer"]
        begin = result["begin_geldigheid"]
        end = result["eind_geldigheid"]

        if src_id == previous.get("src_id") and src_volgnummer == previous.get("src_volgnummer"):
            # begin should be equal to previous end, and nothing can follow a None end
            is_valid = (begin == previous["end"] and previous["end"] is not None)
            if begin is not None and end is not None:
                # If dates are filled then these date should be consecutive
                is_valid = is_valid and end > begin
            if not is_valid and src_id not in gaps:
                logger.warning(f"Inconsistency found for {src_id} relations")
                gaps[src_id] = result
                continue

        no_inconsistencies.append(result)

        previous = {
            "src_id": src_id,
            "src_volgnummer": src_volgnummer,
            "begin": begin,
            "end": end,
        }

    return no_inconsistencies


def relate(catalog_name, collection_name, field_name):  # noqa: C901
    """
    Get all relations for the given catalog, collection and field

    :param catalog_name:
    :param collection_name:
    :param field_name:
    :return: the relations for the given catalog, collection and field
    """
    relations, src_has_states, dst_has_states = get_relations(catalog_name, collection_name, field_name)

    if not relations:
        logger.warning("Warning: No relations found")
        results = []
    else:
        results = _handle_relations(relations)

    results = _remove_gaps(results)

    return results, src_has_states, dst_has_states
