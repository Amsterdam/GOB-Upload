from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(type_=String)
    fullname = Column(String)
    password = Column(String)
    # extra = Column(String)

    def __repr__(self):
        return "<User(name='%s', fullname='%s', password='%s')>" % (
            self.name, self.fullname, self.password)

test_entity = {
    "name": "test_entity",
    "version": "0.1",
    "entity_id": "testid",
    "attributes": {
        "string": {
            "type": "String",
            "description": "String value."
        }
    }
}

def get_type():
    return String

def get_column(type):
    return Column(type)

types = {}
name = "Department"
types[name] = type(name, (Base,), {
    "__tablename__": test_entity["name"],
    "id": Column(Integer, primary_key=True),
    "string": get_column(get_type()),
    "__repr__": lambda self: f"Entity {test_entity['name']}"
})
