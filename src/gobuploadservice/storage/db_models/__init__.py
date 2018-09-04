from gobcore.typesystem import get_gob_type


def get_column(column):
    """Get the SQLAlchemy columndefinition for the gob type as exposed by the gob_type"""
    (column_name, gob_type_name) = column

    gob_type = get_gob_type(gob_type_name)
    return gob_type.get_column_definition(column_name)
