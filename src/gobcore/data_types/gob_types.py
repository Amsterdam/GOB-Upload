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
