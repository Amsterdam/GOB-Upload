"""Enrich.

Enrich incoming messages.

Incoming messages might miss some data.
During enrichment the missing data is added.
"""

import re

from gobcore.model import FIELD
from gobcore.typesystem.gob_geotypes import Geometry

from gobcore.logging.logger import logger

from gobupload import gob_model


class Enricher:

    def __init__(self, storage, msg):
        self.storage = storage
        self.enrich_spec = msg["header"].get("enrich", {})

        # Save any enriched values.
        # Within one session enrichment might depend on previously enriched info for the same message
        # Example: assigning id's should be so that no duplicates are handed out
        self.assigned = {}
        for column, specs in self.enrich_spec.items():
            self.assigned[column] = {
                "issued": {}
            }

            # For autoid, always prefill the last value with the last value in the storage
            if specs["type"] == "autoid":
                find_template = re.sub(r'X*$', '%', specs["template"])
                self.assigned[column]["last"] = storage.get_last_column_value(find_template, column)

        self.enrichers = {
            "geounion": {"func": _geounion, "description": "Generate geometry from other geometries"},
            "autoid": {"func": _autoid, "description": "Generate identifications when missing"}
        }

    def enrich(self, entity):
        for column, specs in self.enrich_spec.items():
            enricher = self.enrichers[specs["type"]]
            value, logging = enricher["func"](
                storage=self.storage, data=entity, specs=specs, column=column, assigned=self.assigned)

            if specs.get("dry_run", False) and value != entity.get(column):
                logger.info(f"Enrich dry run: Generated value {value} for entity {entity[specs['on']]}")
            else:
                entity[column] = value

            if logging:
                logger.info(logging)


def _get_current_value(storage, data, specs, column, assigned):
    """Get any current value (either stored or previously issued.

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


def _geounion(storage, data, specs, column, assigned):
    """Calculate the sum of geometries

    A geometry can consist of the addition of other geometries

    :param storage: Storage handler
    :param data: The data row to process
    :param specs: The geounion specification
    :param column: The column to populate
    :param assigned: Not used
    :return:
    """
    if data.get(column) is not None:
        # Do not overwrite if a value for column already exists
        return data[column], None

    # example: "x.y" => all y values of data[x]
    on_fields = specs["on"].split(".")
    values = list(data[on_fields[0]])
    for on_field in on_fields[1:]:
        values = [value[on_field] for value in values]

    # string quote the results, example [a, b] => ['a', 'b']
    values = [f"'{value}'" for value in values]

    # Derive the table from which to retrieve the geometries
    # Derive the field that is used to match the values with the records in the other table
    catalogue, collection, field = re.split(r'[\:\.]', specs["from"])
    table_name = gob_model.get_table_name(catalogue, collection)

    # Derive the fieldname that contains the geometry in the other table
    geometrie = specs["geometrie"]

    if gob_model.has_states(catalogue, collection):
        # Workaround for collections with (closed) states
        # use the geometry of the highest volgnummer per entity
        # historic entities should be considered, not only the actual ones
        subquery = f"""
            SELECT {FIELD.TID}
            FROM (
                SELECT {FIELD.TID}, {FIELD.SEQNR}, MAX({FIELD.SEQNR}) OVER (PARTITION BY {field}) as max_volgnummer
                FROM {table_name}
                WHERE {field} in ({', '.join(values)})
            ) tids
            WHERE {FIELD.SEQNR} = max_volgnummer
        """
    else:
        subquery = f"""
            SELECT {FIELD.TID}
            FROM {table_name}
            WHERE {field} in ({', '.join(values)})
        """

    query = f"""
SELECT ST_AsText(ST_Union({geometrie}))
FROM {table_name}
JOIN ({subquery}) valid_tids
USING ({FIELD.TID})
"""

    result = storage.get_query_value(query)

    return Geometry.from_value(result).to_value, None


class AutoIdException(Exception):

    def __init__(self, data, specs, column, assigned):
        """
        Exception to raise when an entity with ID
        conflicts with an ID that has previously been issued to an entity without an ID

        :param data:
        :param specs:
        :param column:
        :param assigned:
        """
        value = data[column]    # Conflicting (auto-)id value
        on = specs['on']        # On what field is the auto-id issuing based
        # Find the entity that has already been assigned the conflicting value
        assigned_on = [k for k, v in assigned[column]['issued'].items() if v == value][0]
        # Provide for enough information to solve the issue
        msg = f"Auto-ID value {value} for entity with {on} = {data[on]} " +\
              f"already assigned to entity with {on} = {assigned_on}"
        super().__init__(msg)


def _update_last_assigned(data, specs, column, assigned):
    """
    Use the template to recognize id values that have the same format as an autoid
    Adjust the last issued autoid value if required

    :param data:
    :param specs:
    :param column:
    :param assigned:
    :return:
    """
    autoid_pattern = specs['template'].replace("X", r"\d")
    id = str(data[column])
    if re.match(autoid_pattern, id):
        # id is in the format of an auto-id value
        if not assigned[column]['last'] or (int(id) >= int(assigned[column]['last'])):
            # and no auto-id has yet been issued or the id is bigger that the last issued auto-id
            assigned[column]['last'] = id


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
    column_value = data.get(column)
    if column_value is not None:
        # Do not overwrite if a value for column already exists
        if column_value in assigned[column]["issued"].values():
            # Signal any conflicts with previously assigned values
            # All handling is yielded from one step to another
            # It is unfortunately not possible to re-assign the conflicting entity with a new code
            raise AutoIdException(data, specs, column, assigned)
        _update_last_assigned(data, specs, column, assigned)
        return data[column], None

    on = specs["on"]    # On which column should be searched for an existing value

    current = _get_current_value(storage, data, specs, column, assigned)
    if current:
        # Insert current value and adjust last assigned value if required
        data[column] = current
        _update_last_assigned(data, specs, column, assigned)
        return current, None

    # No value is already stored neither has a value already been issued
    # Get last issued value
    last_value = assigned[column]["last"]
    template = specs["template"]

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

    return value, None
