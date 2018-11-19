from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from gobupload.models.model import Base, User, types

engine = create_engine('sqlite:///example.db', echo=True)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

me = User(name='me', fullname='me me', password='geheim')

session.add(me)
session.commit()

dep = types["Department"](string="s")

session.add(dep)

session.commit()

users = session.query(User).all()
deps = session.query(types["Department"]).all()

print("Users", users)
print("Departments", deps)
