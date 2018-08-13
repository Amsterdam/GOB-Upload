from sqlalchemy.ext.automap import automap_base


def get_reflected_base(engine):
    # prepare base for autoreflecting existing tables
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Base.metadata.reflect(bind=engine)

    return Base
