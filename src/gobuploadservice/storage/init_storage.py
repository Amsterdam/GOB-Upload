"""Storage initialisation

Create database schemas for GOB models on demand.
Any GOB model that has not yet been created will be created automatically

Todo: Separate model from import
    GOB without any imports is also a valid GOB
    However, the current implementation will create the models only after the first import
    Also imports for the same data from multiple sources requires the GOB model to be duplicated

"""
from gobuploadservice.storage.init_entity import init_entity
from gobuploadservice.storage.init_event import init_event
from gobuploadservice.storage.util import get_reflected_base


def init_storage(metadata, engine, base):
    # Todo: think about how this should be managed, only with approval of workflow manager, I guess, or the events
    #       table might even be introduced earlier (on (first) startup of this project)
    if not hasattr(base.classes, metadata.entity):

        # Create events table if not yet exists
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
