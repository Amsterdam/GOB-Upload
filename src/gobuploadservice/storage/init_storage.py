from gobuploadservice.storage.init_entity import init_entity
from gobuploadservice.storage.init_event import init_event
from gobuploadservice.storage.util import get_reflected_base


def init_storage(metadata, engine, base):
    # Todo: think about how this should be managed, only with approval of workflow manager, I guess, or the events
    #       table might even be introduced earlier (on (first) startup of this project)
    if not hasattr(base.classes, metadata.entity):
        if not hasattr(base.classes, 'event'):
            init_event(engine)
        # create table
        init_entity(metadata, engine)

        base = get_reflected_base(engine)

    # Todo: think about how this should be managed, only with approval of workflow manager, I guess
    # Do any migrations if the data is behind in version
    if metadata.version != "0.1":
        # No migrations defined yet...
        raise ValueError("Unexpected version, please write a generic migration here of migrate the import")

    return base
