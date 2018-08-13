"""Events

Each update to the model is by means of an event.
Events can specify to Add, Delete, Change of Confirm any data in the database.
Events are the key data of the database. It allows to (re-)construct the database for any given point in time
"""
from gobcore.data_types import GOB

EVENTS = {
    "eventid": "GOB.Integer",     # Unique identification of the event, numbered sequentially
    "timestamp": "GOB.DateTime",  # datetime when the event as created
    "entity": "GOB.String",       # the entity to which the event need to be applied
    "action": "GOB.String",       # add, change, delete or confirm
    "source": "GOB.String",       # the source of the entity, e.g. DIVA
    "source_id": "GOB.String",    # the id of the entity in the source
    "contents": "GOB.JSON"        # a json object that holds the contents for the action, e.g. the full entity for an Add
}
