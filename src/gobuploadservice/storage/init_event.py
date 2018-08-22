"""Events

Initialise the events table in the storage

Todo: Table names are plural, change table name to events

"""
from sqlalchemy import MetaData, Table

from gobcore.data_types.db_util import get_column_definition
from gobcore.models.event import EVENTS


def init_event(engine):
    """
    Create the events table

    :param engine:
    :return:
    """
    table_name = "event"
    meta = MetaData(engine)

    columns = [get_column_definition(column) for column in EVENTS.items()]
    table = Table(table_name, meta, *columns, extend_existing=True)
    table.create(engine, checkfirst=True)
