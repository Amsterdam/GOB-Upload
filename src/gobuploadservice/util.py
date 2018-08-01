import datetime
from decimal import Decimal


def _is_equal(new_value, stored_value):
    """Determine if a new value is equal to a already stored value

    Values may be stored in a different format than the new data. Convert the data where required

    :param new_value: The new value
    :param stored_value: The currently stored value
    :return: True if both values compare equal
    """
    if isinstance(stored_value, datetime.date):
        return new_value == stored_value.strftime("%Y-%m-%d")
    elif isinstance(stored_value, Decimal):
        return new_value == str(stored_value)
    else:
        return new_value == stored_value


def _calculate_modifications(new_value, cur_value):
    modifications = []
    # Compare each value
    for attr, value in new_value.items():
        if not _is_equal(new_value.get(attr), cur_value.get(attr)):
            # Create a modification request for the changed value
            modifications.append({
                "key": attr,
                "new_value": new_value.get(attr),
                "old_value": cur_value.get(attr)
            })
    return modifications


def calculate_mutation(new_value, cur_value):
    # Determine the appropriate action
    if not cur_value:
        # Entity does not yet exist, create it
        mutation = {
            "action": "ADD",
            "contents": new_value
        }
    elif new_value is None:
        # Entity no longer exists in source
        if cur_value["_date_deleted"] is None:
            # Has the deletion already been processed?
            mutation = {
                "action": "DELETED",
                "contents": id
            }
        else:
            # Do not delete twice
            return None
    else:
        # Modified or Confirmed entry
        modifications = _calculate_modifications(new_value, cur_value)
        if len(modifications) == 0:
            # Every value compares equal, entity is confirmed
            mutation = {
                "action": "CONFIRMED",
                "contents": id
            }
        else:
            # Some values have changed, entity is modified
            mutation = {
                "action": "MODIFIED",
                "contents": {
                    "id": id,
                    "modifications": modifications
                }
            }

    return mutation
