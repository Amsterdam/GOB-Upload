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
import operator

from gobcore.model.metadata import FIELD
from gobcore.logging.logger import logger

from gobupload.storage.relate import DST_MATCH_PREFIX, update_relations, date_to_datetime
# from gobupload.storage.relate import get_relations


# Relations can have missing begin and end dates.
# Begin-of-time and end-of-time are used to cope with this.
_BEGIN_OF_TIME = datetime.datetime.min
_END_OF_TIME = datetime.datetime.max


def _compare_date(value):
    """
    Transform value to datetime to allow date - datetime comparison

    :param value: datetime.date or datetime.datatime
    :return: value as datetime.datetime value
    """
    if not isinstance(value, datetime.datetime):
        return date_to_datetime(value)
    else:
        return value


def _compare_dates(date1, compare, date2):
    """
    Compare two dates.

    If the values have equal type, use the plain comparison function
    Else, transform the value into universally comparable value
    :param date1:
    :param compare: the compare function, e.g. operator.lt
    :param date2:
    :return:
    """
    if type(date1) == type(date2):
        return compare(date1, date2)
    else:
        return compare(_compare_date(date1), _compare_date(date2))


def _get_slots(src, dsts):
    """
    Examine begin and end dates from source and destinations.

    Construct time slots for every timespan found
    :param src:
    :param dsts:
    :return:
    """
    # Collect all dates
    src_begin = src['begin']
    src_end = src['end']

    dates = [src_begin, src_end]
    for dst in dsts:
        dst_begin = dst['begin']
        if _compare_dates(dst_begin, operator.lt, src_begin):  # dst_begin < src_begin
            # Adjust the begin of the relation to the begin of the source
            dst_begin = src_begin

        dst_end = dst['end']
        if _compare_dates(dst_end, operator.gt, src_end):  # dst_end > src_end
            # Adjust the end of the relation to the end of the source
            dst_end = src_end

        dates.extend([dst_begin, dst_end])

    # Unique dates
    dates = list(set(dates))

    # Sorted from oldest to newest
    dates.sort(key=lambda v: _compare_date(v))

    if len(dates) == 1:
        # Allow for intervals that have equal start - end date
        dates = [dates[0], dates[0]]

    # Transform into time slots and return the result
    return [{
        "src": {
            "source": src["source"],
            "id": src["id"],
            "volgnummer": src["volgnummer"]
        },
        "begin_geldigheid": dates[i],
        "eind_geldigheid": dates[i + 1],
        "dst": []
    } for i in range(len(dates) - 1)]


def _post_process_slots(slots, src, dsts):
    """
    Post processing of slots

    Restore original None values for BEGIN-END OF TIME
    Insert null destinations for empty relations

    :param slots:
    :param src:
    :param dsts:
    :return:
    """
    for slot in slots:
        # Restore original None values for BEGIN-END OF TIME
        if slot["begin_geldigheid"] == _BEGIN_OF_TIME:
            slot["begin_geldigheid"] = None
        if slot["eind_geldigheid"] == _END_OF_TIME:
            slot["eind_geldigheid"] = None

        # Insert null destinations for empty relations
        if not slot["dst"]:
            slot["dst"] = [{
                "source": dsts[0]["source"],
                "id": None,
                "volgnummer": None,
                "method": None,
                "match": None
            }]

    return slots


def _end_source(src, dsts):
    """
    Finish the relations of a given source

    The related destinations are assigned to timeslots and the result is returned

    :param src:
    :param dsts:
    :return:
    """
    if not dsts:
        return []

    # Get all time slots (begin-end in ascending order)
    slots = _get_slots(src, dsts)

    for dst in dsts:
        dst_begin = dst['begin']
        dst_end = dst['end']
        # Add to relevant slots
        for slot in slots:
            # dst_begin <= slot["begin_geldigheid"] and dst_end >= slot["eind_geldigheid"]
            if _compare_dates(dst_begin, operator.le, slot["begin_geldigheid"]) and \
               _compare_dates(dst_end, operator.ge, slot["eind_geldigheid"]):
                # destination matches time slot
                item = {
                    "source": dst["source"],
                    "id": dst["id"],
                    "volgnummer": dst["volgnummer"],
                    "method": dst["method"],
                    "match": dst["match"]
                }
                if dst.get("bronwaardes"):
                    # Register the values on which the match was made
                    # In the matched item
                    item["bronwaardes"] = dst["bronwaardes"]
                    # And in the source
                    slot["src"]["bronwaardes"] = slot["src"].get("bronwaardes", [])
                    slot["src"]["bronwaardes"].extend(dst["bronwaardes"])
                slot["dst"].append(item)

    # Clean up time slots and return the result
    return _post_process_slots(slots, src, dsts)


def _get_record(row):
    """
    Transforms a row into a record.

    Each record has a source and related destination

    :param row:
    :return:
    """
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
            "end": row.get(f"dst_{FIELD.END_VALIDITY}"),
            "method": row["method"],
            "match": row["match"]
        }
    }

    # Set None dates to begin and end of time to allow date(time) comparison
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


def _handle_relations(rows):
    """
    Process each row and return the collection of relations

    :param rows:
    :return:
    """
    # One source may have multiple relations
    src = None
    dsts = []

    # Detect change of source
    previous_id = None

    for row in rows:
        record = _get_record(row)
        id = f"{record['src']['source']}.{record['src']['id']}.{record['src']['volgnummer']}"

        if id != previous_id:
            # Close previous
            yield from _end_source(src, dsts)
            # Start new
            src = record['src']
            dsts = []

        dsts.append(record['dst'])
        previous_id = id

    # Close last
    yield from _end_source(src, dsts)


def relate(catalog_name, collection_name, field_name):
    """
    Get all relations for the given catalog, collection and field

    :param catalog_name:
    :param collection_name:
    :param field_name:
    :return: the relations for the given catalog, collection and field
    """
    relations, src_has_states, dst_has_states = update_relations(catalog_name, collection_name, field_name)
    # relations, src_has_states, dst_has_states = get_relations(catalog_name, collection_name, field_name)

    results = _handle_relations(relations)
    if not results:
        logger.warning("Warning: No relations found")

    return results, src_has_states, dst_has_states
