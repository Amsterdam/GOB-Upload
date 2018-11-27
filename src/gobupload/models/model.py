from sqlalchemy import Index
from sqlalchemy.ext.declarative import declarative_base

# Import data definitions
from gobcore.model import GOBModel
from gobcore.model.metadata import FIXED_COLUMNS, METADATA_COLUMNS, STATE_COLUMNS
from gobupload.storage.db_models.event import EVENTS

# Utility method to convert GOB type to a SQLAlchemy Column
from gobupload.storage.db_models import get_column

# Store all model classes
models = {
    # e.g. "meetbouten_rollagen": <BASE>
}

Base = declarative_base()


def _derive_models():

    def columns_to_model(table_name, columns):
        # Convert columns to SQLAlchemy Columns
        columns = {column_name: get_column((column_name, column_type)) for column_name, column_type in columns.items()}

        # Create model
        models[table_name] = type(table_name, (Base,), {
            "__tablename__": table_name,
            **columns,
            "__repr__": lambda self: f"{table_name}"
        })

    model = GOBModel()

    # Start with events
    columns_to_model("events", EVENTS)

    for catalog_name, catalog in model.get_catalogs().items():
        for collection_name, collection in model.get_collections(catalog_name).items():
            if collection['version'] != "0.1":
                # No migrations defined yet...
                raise ValueError("Unexpected version, please write a generic migration here or migrate the import")

            # "attributes": {
            #     "attribute_name": {
            #         "type": "GOB type name, e.g. GOB.String",
            #         "description": "attribute description",
            #         "ref: "collection_name:entity_name, e.g. meetbouten:meetbouten"
            #     }, ...
            # }

            # the GOB model for the specified entity
            fields = {col: desc['type'] for col, desc in collection['fields'].items()}

            state_columns = STATE_COLUMNS if collection.get('has_states') else {}

            # Collect all columns for this collection
            entity = {
                **FIXED_COLUMNS,
                **METADATA_COLUMNS['private'],
                **METADATA_COLUMNS['public'],
                **state_columns,
                **fields
            }

            table_name = model.get_table_name(catalog_name, collection_name)
            columns_to_model(table_name, entity)
            Index(
                f'{table_name}.idx.source_source_id',
                '_source',
                '_source_id',
                unique=True
            )


_derive_models()
