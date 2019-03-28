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

from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobcore.logging.logger import logger

from gobupload.storage.relate import DST_MATCH_PREFIX, get_relations, date_to_datetime


# Relations can have missing begin and end dates.
# Begin-of-time and end-of-time are used to cope with this.
_BEGIN_OF_TIME = datetime.datetime.min
_END_OF_TIME = datetime.datetime.max


def print_result(result):
    print(f"{result['begin_geldigheid']} - {result['eind_geldigheid']} {[dst['id'] for dst in result['dst']]}")


def _handle_state_relation(state_results, state, relation, next_begin):
    """
    Process a state (src) with its relations

    :param state:
    :param relation:
    :return:
    """
    # print("State", state)
    # print("Relation", relation)
    # print("Next begin", next_begin)
    if relation.get('begin_geldigheid', 'begin') == relation.get('eind_geldigheid', 'eind'):
        results = []
    else:
        if relation.get('begin_geldigheid') == _BEGIN_OF_TIME:
            relation['begin_geldigheid'] = None
        results = [{
            "src": state["src_id"],
            "begin_geldigheid": relation.get('begin_geldigheid'),
            "eind_geldigheid": relation.get('eind_geldigheid'),
            "dst": relation['dst']
        }]

    if relation.get('eind_geldigheid') != next_begin:
        if relation.get('eind_geldigheid') is None:
            last_result = results.pop()
            last_result["eind_geldigheid"] = next_begin
            results.append(last_result)
            if state_results:
                last_state_result = state_results.pop()
                # print("Last result", last_state_result)
                state_results.append(last_state_result)
            # print("Last", last_result)
        else:
            # Fill any gap
            results.append({
                "src": state["src_id"],
                "begin_geldigheid": relation.get('eind_geldigheid'),
                "eind_geldigheid": next_begin,
                "dst": [_no_dst(relation['dst'][0]['source'])]
            })

    for result in results:
        dst = result['dst']
        not_none_items = [d for d in dst if d['id'] is not None]
        if len(not_none_items) >= 1:
            # Do not allow empty results when the list of dst's has valid dst items
            result['dst'] = not_none_items

    state_results.extend(results)


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
                _handle_state_relation(results, state, relation, relation.get('eind_geldigheid'))
                relation['dst'] = []
                # Adjust row
                row['begin_geldigheid'] = relation.get('eind_geldigheid')
        elif relation:
            # End state
            _handle_state_relation(results, state, relation, row['begin_geldigheid'])
        relation = row

    if relation:
        _handle_state_relation(results, state, relation, relation.get('eind_geldigheid'))

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
    result = {
        "source": row[source],
        "id": row[id],
        "volgnummer": row.get(volgnummer)
    }
    source_values = [value for key, value in row.items() if DST_MATCH_PREFIX in key]
    if source_values:
        result["bronwaardes"] = source_values
    return result


def _close_state(state, relations, previous, results):
    """
    If the source has a longer lifetime than the destination add an empty relation at the end

    :param state:
    :param relations:
    :param previous_end:
    :param results:
    :return:
    """
    if not relations:
        return []

    if relations:
        dst_end = previous["dst_end"]
        if dst_end != state["end"]:
            # Add an empty last relation
            no_dst = _no_dst(previous["dst_source"])
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


def _new_close_state(src, dsts):
    if not dsts:
        return []

    src_begin = src['begin']
    src_end = src['end']

    # Collect all dates
    dates = [src["begin"], src["end"]]
    for dst in dsts:
        dst_begin = dst['begin']
        if dst_begin.isoformat() < src_begin.isoformat():
            dst_begin = src_begin

        dst_end = dst['end']
        if dst_end.isoformat() > src_end.isoformat():
            dst_end = src_end

        dates.extend([dst_begin, dst_end])

    # Unique dates
    dates = list(set(dates))

    # Sorted from oldest to newest
    dates.sort(key=lambda v: v.isoformat())

    slots = [{
            "src": {
                "source": src["source"],
                "id": src["id"],
                "volgnummer": src["volgnummer"]
            },
            "begin_geldigheid": dates[i],
            "eind_geldigheid": dates[i + 1],
            "dst": []
        } for i in range(len(dates) - 1)]

    for dst in dsts:
        dst_begin = dst['begin']
        dst_end = dst['end']
        # Add to relevant slots
        for slot in slots:
            if dst_begin.isoformat() <= slot["begin_geldigheid"].isoformat() and \
                    dst_end.isoformat() >= slot["eind_geldigheid"].isoformat():
                item = {
                    "source": dst["source"],
                    "id": dst["id"],
                    "volgnummer": dst["volgnummer"]
                }
                if dst.get("bronwaardes"):
                    item["bronwaardes"] = dst["bronwaardes"]
                    slot["src"]["bronwaardes"] = slot["src"].get("bronwaardes", [])
                    slot["src"]["bronwaardes"].extend(dst["bronwaardes"])
                slot["dst"].append(item)

    for slot in slots:
        if slot["begin_geldigheid"] == _BEGIN_OF_TIME:
            slot["begin_geldigheid"] = None
        if slot["eind_geldigheid"] == _END_OF_TIME:
            slot["eind_geldigheid"] = None
        if not slot["dst"]:
            slot["dst"] = [{
                "source": dsts[0]["source"],
                "id": None,
                "volgnummer": None
            }]

    return slots


def _get_record(row):
    record = {
        "src": {
            "source": row[f"src_{FIELD.SOURCE}"],
            "id": row[f"src_{FIELD.ID}"],
            "volgnummer": row.get(f"src_{FIELD.SEQNR}"),
            "begin": row.get(f"src_{FIELD.START_VALIDITY}"),
            "end": row.get(f"src_{FIELD.END_VALIDITY}")
        },
        "dst": {
            "source": row[f"dst_{FIELD.SOURCE}"],
            "id": row[f"dst_{FIELD.ID}"],
            "volgnummer": row.get(f"dst_{FIELD.SEQNR}"),
            "begin": row.get(f"dst_{FIELD.START_VALIDITY}"),
            "end": row.get(f"dst_{FIELD.END_VALIDITY}")
        }
    }

    # Set None dates to begin and end of time to allow comparison
    for item in ["src", "dst"]:
        if record[item]["begin"] is None:
            record[item]["begin"] = _BEGIN_OF_TIME
        if record[item]["end"] is None:
            record[item]["end"] = _END_OF_TIME

    # Include the matches values if available
    match_values = [value for key, value in row.items() if DST_MATCH_PREFIX in key]
    if match_values:
        record["dst"]["bronwaardes"] = match_values

    return record


def _new_handle_relations(rows):
    results = []

    src = None
    dsts = []
    previous_id = None

    for row in rows:
        record = _get_record(row)
        id = f"{record['src']['source']}.{record['src']['id']}.{record['src']['volgnummer']}"

        if id != previous_id:
            # Close previous
            results.extend(_new_close_state(src, dsts))
            # Start new
            src = record['src']
            dsts = []

        dsts.append(record['dst'])
        previous_id = id

    # Close last
    results.extend(_new_close_state(src, dsts))

    return results


def _handle_relations(rows, multi=False):
    """
    The relation data that is retrieved from the database is transformed into relation data
    with timeslots that cover the complete lifetime of the source field.

    :param rows: database query results
    :return: array with relations ordered by timeslot
    """
    return _new_handle_relations(rows)

    state = {}
    previous = {}
    relations = []
    results = []
    for row in rows:
        # src._source, src._id, src.volgnummer, src.begin_geldigheid, dst.begin_geldigheid, dst.eind_geldigheid
        # Get the source specs
        src = _get_src_id(row)
        src_id = _get_id(row, f"src_{FIELD.SOURCE}", f"src_{FIELD.ID}", f"src_{FIELD.SEQNR}")
        src_begin = row.get(f"src_{FIELD.START_VALIDITY}")
        src_end = row.get(f"src_{FIELD.END_VALIDITY}")

        # Get the destination specs
        dst_id = _get_id(row, f"dst_{FIELD.SOURCE}", f"dst_{FIELD.ID}", f"dst_{FIELD.SEQNR}")
        dst_begin = row.get(f"dst_{FIELD.START_VALIDITY}", src_begin)
        dst_end = row.get(f"dst_{FIELD.END_VALIDITY}", src_end)

        src_time = f"{src_begin} - {src_end}"
        dst_time = f"{dst_begin} - {dst_end}"

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
            # dst_begin = _add_relations_before_dst_begin(src_begin, dst_begin, dst_id, relations)

        if src != previous.get('src') or (dst_time == previous.get('dst_time')):
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
            "dst_end": dst_end,
            "dst_time": dst_time
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
                print("_____")
                print(begin)
                print(previous["end"])
                extra_data = {
                    'id': "inconsistency found",
                    'data': {
                        'identificatie': src_id,
                        'volgnummer': src_volgnummer
                    }
                }
                logger.warning(f"Inconsistency found", extra_data)
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


def _get_field_type(catalog_name, collection_name, field_name):
    model = GOBModel()
    collection = model.get_collection(catalog_name, collection_name)
    return collection['all_fields'][field_name]['type']


def relate(catalog_name, collection_name, field_name):
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
        multi = _get_field_type(catalog_name, collection_name, field_name) == "GOB.ManyReference"
        results = _handle_relations(relations, multi=multi)

    results = _remove_gaps(results)

    return results, src_has_states, dst_has_states
