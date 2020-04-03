from gobcore.model import GOBModel, FIELD
from gobcore.typesystem import get_modifications
from gobcore.events import get_event_for

from gobupload.storage.handler import GOBStorageHandler

from gobupload.compare.populate import Populator
from gobupload.compare.enrich import Enricher


def _get_event(entity: dict, entity_model: dict, storage: GOBStorageHandler):
    with storage.get_session():
        current = storage.get_current_entity(entity)

    modifications = []
    if current and getattr(current, FIELD.HASH) != entity[FIELD.HASH]:
        # Current entity exist and has changed, get modifications
        modifications = get_modifications(current, entity, entity_model['all_fields'])

    return get_event_for(current, entity, modifications)


def get_events(msg, storage, entities):
    catalog = storage.metadata.catalogue
    collection = storage.metadata.entity

    # Get entity model
    gob_model = GOBModel()
    entity_model = gob_model.get_collection(catalog, collection)

    events = []
    for entity in entities:
        # Enrich and Populate to add _id, _version and _hash
        Enricher(storage, msg).enrich(entity)
        Populator(entity_model, msg).populate(entity)

        # Derive the event and add to events collection
        events.append(_get_event(entity, entity_model, storage))
    return events
