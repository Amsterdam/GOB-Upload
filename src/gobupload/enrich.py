"""Enrich

Enrich incoming messages

Incoming messages might miss some data.
During enrichment the missing data is added

"""
import re


def enrich(storage, msg, logger):
    """
    Enrich message msg
    A storage handler is provided for access to the current storage

    A logger is supplied to log info, warnings and errors

    :param storage: Storage handler
    :param msg: Incoming message
    :param logger: Logger instance
    :return: None
    """
    enrich = msg["header"].get("enrich", {})

    # Save any enriched values.
    # Within one session enrichment might depend on previously enriched info for the same message
    # Example: assigning id's should be so that no duplicates are handed out
    assigned = {}
    for column, specs in enrich.items():
        assigned[column] = {
            "last": None,
            "issued": {}
        }

    for data in msg["contents"]:
        for column, specs in enrich.items():
            # For now only autoid is supported
            assert specs["type"] == "autoid", f"Type '{specs['type']}' invalid; only autoid enrichment is supported"
            data[column], logging = _autoid(storage=storage, data=data, specs=specs, column=column, assigned=assigned)
            if logging:
                logger.info(logging)


def _get_current_value(storage, data, specs, column, assigned):
    """
    Get any current value (either stored or previously issued

    :param storage: Storage handler
    :param data: The data row to process
    :param specs: The autoid specification (on (which column), template (to build a new id))
    :param column: The column to populate (normally identification)
    :param assigned: Any already assigned values
    :return: value, or None if no current value exists
    """
    on = specs["on"]    # On which column should be searched for an existing value

    # Check if a current value already exists in the storage
    current = storage.get_column_values_for_key_value(column, on, data[on])     # Get any current value
    if current:
        # Only one value for the on column should exist
        assert len(current) == 1, f"Multiple values for {column} found for {on} = '{data[on]}'"
        # Use the current value as the new value
        return getattr(current[0], column)

    # Check if a value has already been issued
    issued = assigned[column]["issued"].get(data[on])
    if issued:
        # Use the value that already has been issued
        return issued


def _autoid(storage, data, specs, column, assigned):
    """
    Auto add id if missing

    :param storage: Storage handler
    :param data: The data row to process
    :param specs: The autoid specification (on (which column), template (to build a new id))
    :param column: The column to populate (normally identification)
    :param assigned: Any already assigned values
    :return:
    """
    if data[column] is not None:
        # Do not overwrite if a value for column already exists
        return data[column], None

    on = specs["on"]    # On which column should be searched for an existing value

    current = _get_current_value(storage, data, specs, column, assigned)
    if current:
        return current, None

    # No value is already stored neither has a value already been issued
    # Get last issued value
    last_value = assigned[column]["last"]
    template = specs["template"]
    if not last_value:
        # If no last issued value exist, check the storage for any last value with the same template
        find_template = re.sub(r'X*$', '%', template)
        last_value = storage.get_last_column_value(find_template, column)

    if last_value:
        # Create an id that follows the last value
        max_value = int(re.sub(r'X', '9', template))
        value = int(last_value) + 1
        assert value <= max_value, f"Maximum value {max_value} for column {column} has been reached"
        value = str(value).rjust(len(template), '0')
    else:
        # Start with a fresh id range
        start_value = re.sub(r'X', '0', template)
        value = start_value

    assigned[column]["issued"][data[on]] = value    # Register the issuance for the give 'on' value
    assigned[column]["last"] = value    # Register that last issued value

    logging = f"{specs['type']}: {column} = '{value}' for {on} = '{data[on]}'"

    return value, logging
