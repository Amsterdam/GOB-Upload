# Event handling

## Validity check

An important step is to check if the events that have been received are valid
(The handling of events is idempotent).

Events result from a comparison between the actual state of an entity and the newly received state of an entity.
The actual state of an entity is identified by its "Last Event" property.
This denotes the id of the last event that has "touched" the entity.
The newly received event contains this identification.
A valid event has a "Last Event" property that is equal to the "Last Event" property of the entity.

## Model check

Before the new events can be processed the actual state of the entities is compared with the events table.
If everything is OK then the id of the last event in the events table is equal to the highest "Last Event" property of the entities.

Any previously failed upload is detected when the last event in the events table is higher that the highest "Last Event" property of the entities.
In this case the model is updated with the missing events.
The new upload will be rejected as it will normally never result in valid events
because the "Last Event" property of the entities will be modified by the update of the missing events.

## Store events

Next step is to store the events in the events table

## Apply events

Last step is to apply the newly stored events to the entities.

### CONFIRM events

CONFIRM (and BULKCONFIRM) events are handled differently from the other events.
They do only set the "Last Confirmed" property of the entity.
The "Last Event" property of the entity remains unchanged.

After all events have been applied the CONFIRM events are deleted from the events table.

The "Last Event" property therefore notices the id of the last event that has changed the entity (ADD, MODIFY, DELETE).

The reason to store and apply the CONFIRM events as all other entities is to improve the robustness.
Any failure in the handling of events will be noticed in the next upload.
The unprocessed deletions of the CONFIRM events will lead to an outdated model which will first be updated.
