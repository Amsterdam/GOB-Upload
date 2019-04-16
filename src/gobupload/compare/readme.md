# Compare

## Collect

Incoming entities are processed one by one.
Each entity is first enriched, then populated with a hash value and then stored into a temporary table.

Enrichment is used to:
- add a geometry field to an entity by uniting geometries of entities in other collections.  
An example of this enrichment is the geometry of GGWGebieden. A GGWGebied consists of buurten and its geometry is the union of the geometry of its buurten
- provide for a missing identification  
If source systems do not provide for an id, the id can be determined in the enrichment step.  
Id generation is based upon a template. The last issued value in a collection together with the template is used to construct a new id.

Population is used to add an _id field to an entity (_id is the universal identification for each entity), the version (_version) and a hash (_hash). The hash is used to allow for fast comparison of entities.

Statistics are collected during the process for evaluation purposes.

Each entity is finally stored into a temporary table.
The temporary table is used to query for the differences between the new and the current entities.

## Exception for initial loads

An exception is made for the initial load of a collections.
If no current entities exist the entities are not stored into a temporary table.
Each new entity is converted into an ADD event instead of being stored in a temporary table.

## Comparison

The entities are compared using a database query on the temporary and actual table.

Entities are queried for having equal functional id and source (possibly including sequence number of entities have state) and the result is the technical source id of the new and existing entity and the event type (ADD, MODIFY, DELETE, CONFIRM, SKIP).

- ADD is when no current entity exist, or when a current entity exists that has previously been deleted
- DELETE is when no new entity exists
- SKIP is when no new entity exists and the current entity has already been deleted
- CONFIRM is when the new and current entity have the same hash
- MODIFY is all other cases

Each result of the query is interpreted, converted into an event and stored in a file.

Mulitple CONFIRM events are grouped into BULKCONFIRM events to improve performance.

The result of the compare process is a list of events that is stored in a file to be further processed.
