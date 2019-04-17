# Update

The first step in the update process is to check if the model is up to date.

As each entity is linked to the last event that has “touched” the entity,
the check consists of comparing the highest event id of the entities to be updated,
with the id of the last event for these entities.

If the ids don’t match the update is not processed.
- If the highest event id of the entities is higher that the id of the last event a critical inconsistency is reported.  
Entities can never be newer than their events.
- If the highest event id of the entities is lower than the id of the last event the model will be updated with the missing events.  
As this will invalidate the new events, further processing of the new events will not be done.

If the model is up to date, the first step is to get a list of [source id - last event] combinations.
This list is used to check individual events for validity and to recognize add events for new entities.

## Store events
Events will first be stored in the events table.

During this process events will be checked for validity:  
Each event knows the last_event property of the entity against which it has been compared.
An event is considered valid if this matches.
This assures that the event is derived from the entity as it currently exists in the database.  

Only valid events will be stored.

## Apply events
After the events have been stored the events will be applied on the existing entities.  

During application of the events new entities are recognized by having no [source id – last event] combination.
These events are grouped and inserted in bulk to improve performance.
