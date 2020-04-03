from gobcore.events.import_events import CONFIRM

from gobupload.storage.handler import GOBStorageHandler


class UpdateException(Exception):
    pass


def store_events(storage: GOBStorageHandler, events):
    if [event['event'] for event in events if event['event'] == CONFIRM.name]:
        raise UpdateException("CONFIRM events are not stored")

    with storage.get_session():
        storage.add_events(events)
