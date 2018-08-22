"""Events

Each update to the model is by means of an event.
Events can specify to Add, Delete, Change of Confirm any data in the database.
Events are the key data of the database. It allows to (re-)construct the database for any given point in time

Todo: GOB. type logic encapsulation
    The type definitions of the EVENTS attributes use logic defined in data_types to reference a GOB type
    The concatenation of "GOB." and the typename is logic that should be shared instead of duplicated

"""
import copy

EVENTS = {
    "eventid": "GOB.PKInteger",   # Unique identification of the event, numbered sequentially
    "timestamp": "GOB.DateTime",  # datetime when the event as created
    "entity": "GOB.String",       # the entity to which the event need to be applied
    "action": "GOB.String",       # add, change, delete or confirm
    "source": "GOB.String",       # the source of the entity, e.g. DIVA
    "source_id": "GOB.String",    # the id of the entity in the source
    "contents": "GOB.JSON"        # a json object that holds the contents for the action, the full entity for an Add
}


def create_event(orm_event, gob_event, data, metadata):
    """
    Method to fill the orm event entity with the required data,
    specifically placed here, to make sure all fields above are filled

    :param orm_event:
    :param gob_event:
    :param data:
    :param metadata:
    :return: orm_event
    """

    return orm_event(
        timestamp=metadata.timestamp,
        entity=metadata.entity,
        action=gob_event.name,
        source=metadata.source,
        source_id=data[metadata.source_id_column],
        contents=copy.deepcopy(data)
    )
