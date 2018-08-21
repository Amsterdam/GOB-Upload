"""GOB Data types

GOB data consists of entities with attributes, eg Meetbout = { meetboutidentificatie, locatie, ... }
The possible types for each attribute are defined in this module.
The definition and characteristics of each type is in the gob_types module

"""
from gobcore.data_types import gob_types

# The possible type definitions are imported from the gob_types module
GOB = gob_types

# The actual types that are used within GOB
GOB_TYPES = [
    GOB.String,
    GOB.Character,
    GOB.Integer,
    GOB.PKInteger,
    GOB.Number,
    GOB.Decimal,
    GOB.Date,
    GOB.DateTime,
    GOB.JSON,
    GOB.Boolean
]

# Convert GOB_TYPES to a dictionary indexed by the name of the type
_gob_types_dict = {f'GOB.{gob_type.__name__}': gob_type for gob_type in GOB_TYPES}

# no geo implemented yet. We pass wkt strings around for now.
# todo: Implement geo
_gob_types_dict['GOB.Geo.Point'] = GOB.String


def get_gob_type(name):
    """
    Get the type definition for a given type name

    Example:
        get_gob_type("string") => GOBType:String

    :param name:
    :return: the type definition (class) for the given type name
    """
    return _gob_types_dict[name]
