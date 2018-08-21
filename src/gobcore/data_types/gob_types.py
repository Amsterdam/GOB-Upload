"""GOB Types

Each possible data type in GOB is defined in this module.
The definition includes:
    name - the name of the data type, e.g. string
    sql_type - the corresponding storage type, e.g. sqlalchemy.String
    is_pk - whether the attribute that has this type is the primary key of the entity

todo:
    is_pk tells something about the attribute, not about the type.
    should is_pk stay a type property?

todo:
    GOBType is an abstract base class, why not subclass from ABC?

"""
import sqlalchemy


class GOBType():
    is_pk = False
    name = "type"
    sql_type = sqlalchemy.Column


class String(GOBType):
    name = "string"
    sql_type = sqlalchemy.String


class Character(GOBType):
    name = "character"
    sql_type = sqlalchemy.CHAR


class Integer(GOBType):
    name = "integer"
    sql_type = sqlalchemy.Integer


class PKInteger(Integer):
    is_pk = True


class Decimal(GOBType):
    name = "decimal"
    sql_type = sqlalchemy.DECIMAL


class Number(GOBType):
    name = "number"
    sql_type = sqlalchemy.NUMERIC


class Date(GOBType):
    name = "date"
    sql_type = sqlalchemy.Date


class DateTime(GOBType):
    name = "datetime"
    sql_type = sqlalchemy.DateTime


class JSON(GOBType):
    name = "json"
    sql_type = sqlalchemy.JSON


class Boolean(GOBType):
    name = "boolean"
    sql_type = sqlalchemy.Boolean
