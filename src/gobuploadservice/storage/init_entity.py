"""Initialisation of the data entity

The database model is maintained in the models module.
This module is used to dynamically generate the physical database.
SQLAlchemy is used as a database abstraction layer.

"""
from gobcore.data_types.db_util import get_column_definition
from gobcore.models.metadata import FIXED_COLUMNS, METADATA_COLUMNS

from sqlalchemy import MetaData
from sqlalchemy import Table, Index


def init_entity(metadata, engine):
    table_name = metadata.entity
    model = metadata.gob_model

    # internal columns
    columns = [get_column_definition(column) for column in FIXED_COLUMNS.items()]
    columns.extend([get_column_definition(column) for column in METADATA_COLUMNS['private'].items()])

    # externally visible columns
    columns.extend([get_column_definition(column) for column in METADATA_COLUMNS['public'].items()])
    data_column_desc = {col: desc['type'] for col, desc in model.items()}
    columns.extend([get_column_definition(column) for column in data_column_desc.items()])

    # Create an index on source and source_id for performant updates
    index = Index(f"{table_name}.idx.source_source_id", "_source", "_source_id", unique=True)

    meta = MetaData(engine)

    table = Table(table_name, meta, *columns, index, extend_existing=True)
    table.create(engine, checkfirst=True)
