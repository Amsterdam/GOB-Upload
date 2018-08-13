from gobcore.data_types import gob_types

GOB = gob_types

GOB_TYPES = [GOB.String, GOB.Character, GOB.Integer, GOB.PKInteger, GOB.Number,
             GOB.Decimal, GOB.Date, GOB.DateTime, GOB.JSON, GOB.Boolean]
_gob_types_dict = {f'GOB.{gob_type.__name__}': gob_type for gob_type in GOB_TYPES}

# no geo implemented yet. We pass wkt strings around for now.
_gob_types_dict['GOB.Geo.Point'] = GOB.String


def get_gob_type(name):
    return _gob_types_dict[name]
