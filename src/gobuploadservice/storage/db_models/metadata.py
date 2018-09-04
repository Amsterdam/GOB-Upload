"""GOB metadata

Definition of the attributes that are automatically added to each GOB entity.

The auto-included attributes have the purpose of
    registering the last application of a specific event, e.g. date_confirmed
    registering the source of the data; source and source_id
    provide for a internal id and universal id reference; gobid and id

"""

"""Meta data that is registered for every entity"""
METADATA_COLUMNS = {
    # Public meta information about each row in a table
    "public": {
        "_version": "GOB.String",
        "_date_created": "GOB.DateTime",
        "_date_confirmed": "GOB.DateTime",
        "_date_modified": "GOB.DateTime",
        "_date_deleted": "GOB.DateTime",
    },

    # These properties will not be made public by the API
    "private": {
        "_source": "GOB.String",
        "_source_id": "GOB.String"
    }
}

"""Columns that are at the start of each entity"""
FIXED_COLUMNS = {
    "_gobid": "GOB.PKInteger",  # The internal (GOB) id of the entity
    "_id": "GOB.String"         # Provide for a generic (independent from Stelselpedia) id field for every entity
}
