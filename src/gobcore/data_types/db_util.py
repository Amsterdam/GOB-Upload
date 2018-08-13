from sqlalchemy import Column

from gobcore.data_types import get_gob_type


def get_column_definition(column):
    """Takes a column definition of the models module and create a corresponding SQLAlchemy Column()

    :param column: a column definition (name, type)
    :return: the corresponding SQLAlchemy Column()
    """
    (column_name, column_type_name) = column
    column_type = get_gob_type(column_type_name)
    return Column(column_name, column_type.sql_type, primary_key=column_type.is_pk, autoincrement=column_type.is_pk)
