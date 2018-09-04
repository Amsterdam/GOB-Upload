"""Events

Initialise the events table in the storage

Todo: Table names are plural, change table name to events

"""
from sqlalchemy import MetaData, Table
from gobuploadservice.storage.db_models import get_column

from gobuploadservice.storage.db_models.event import EVENTS


def init_event(engine):
    """
    Create the events table

    :param engine:
    :return:
    """
    table_name = "event"
    meta = MetaData(engine)

    columns = [get_column(column) for column in EVENTS.items()]
    table = Table(table_name, meta, *columns, extend_existing=True)
    table.create(engine, checkfirst=True)
