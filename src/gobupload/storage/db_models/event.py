"""Events

Each update to the model is by means of an event.
Events can specify to Add, Delete, Change of Confirm any data in the database.
Events are the key data of the database. It allows to (re-)construct the database for any given point in time

Todo: GOB. type logic encapsulation
    The type definitions of the EVENTS attributes use logic defined in data_types to reference a GOB type
    The concatenation of "GOB." and the typename is logic that should be shared instead of duplicated

"""
import copy
import json

from gobcore.typesystem.json import GobTypeJSONEncoder


def build_db_event(DbEvent, event, metadata):
    """
    Method to fill the orm event entity with the required data,
    specifically placed here, to make sure all fields above are filled

    :param orm_event:
    :param event:
    :param data:
    :param metadata:
    :return: orm_event
    """

    # todo: is this the right place to get the id (and event-name)?
    #   this is implicit knowledge of event data structure
    source_id = event['data'][metadata.source_id_column]
    # Use the GOBType encoder to encode the Decimal values
    json_contents = json.dumps(copy.deepcopy(event['data']), cls=GobTypeJSONEncoder)
    return DbEvent(
        timestamp=metadata.timestamp,
        catalogue=metadata.catalogue,
        entity=metadata.entity,
        version=metadata.version,
        action=event['event'],
        source=metadata.source,
        source_id=source_id,
        # todo: should this be named data, instead of contents
        # (contents is part of message, data is part of event)
        contents=json_contents
    )
