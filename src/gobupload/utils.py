import gc
import random
import string
from typing import Any, Tuple

from gobcore.message_broker.offline_contents import ContentsReader, \
    _CONTENTS, _CONTENTS_READER, _MESSAGE_BROKER_FOLDER, _CONTENTS_REF
from gobcore.utils import get_filename


class ActiveGarbageCollection:

    def __init__(self, title):
        assert gc.isenabled(), "Garbage collection should be enabled"
        self.title = title

    def __enter__(self):
        self._collect("start")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._collect("completion")

    def _collect(self, step):
        n = gc.collect()
        if n > 0:
            print(f"{self.title}: freed {n} unreachable objects on {step}")


def is_corrupted(entity_max_eventid, last_eventid):
    if last_eventid is None and entity_max_eventid is None:
        # no events, no entities
        return False
    elif last_eventid is not None and entity_max_eventid is None:
        # events but no data (apply has failed or upload has been aborted)
        return False
    elif entity_max_eventid is not None and last_eventid is None:
        # entities but no events (data is corrupted)
        return True
    elif entity_max_eventid is not None and last_eventid is not None:
        # entities and events, entities can never be newer than events
        return entity_max_eventid > last_eventid


def get_event_ids(storage):
    """Get the highest event id from the entities and the eventid of the most recent event

    :param storage: GOB (events + entities)
    :return:highest entity eventid and last eventid
    """
    with storage.get_session():
        entity_max_eventid = storage.get_entity_max_eventid()
        last_eventid = storage.get_last_eventid()
        return entity_max_eventid, last_eventid


def random_string(length):
    """Returns a random string of length :length: consisting of lowercase characters and digits

    :param length:
    :return:
    """
    assert length > 0
    characters = string.ascii_lowercase + ''.join([str(i) for i in range(10)])
    return ''.join([random.choice(characters) for _ in range(length)])


def fix_xcom_data(xcom_msg_data: dict[str, Any]):
    """Add missing keys to incoming msg data.

    TODO: validate message before sending it in import, also validate it on
          read. XComDataStore should be used for that. Or move this to import.

    :param xcom_msg_data: data retrieved via xcom
    :return: A dict with message data
    """
    if "contents" not in xcom_msg_data:
        xcom_msg_data["contents"] = {}

    return xcom_msg_data


def load_offloaded_message_data(xcom_msg_data: dict[str, Any]) -> Tuple[dict[str, Any], str]:
    """Load offloaded XCom message data and remove reference to offloaded file.

    Based on gob-core's load_message, except that this does not require a
    'converter', or any params. This function just assumes streaming files.

    :param xcom_msg_data: message as received from xcom
    :return: A dictionary with the content as an iterator and the filename.
    """
    filename = get_filename(xcom_msg_data[_CONTENTS_REF], _MESSAGE_BROKER_FOLDER)
    reader = ContentsReader(filename)
    xcom_msg_data[_CONTENTS] = reader.items()
    xcom_msg_data[_CONTENTS_READER] = reader
    # Remove offloaded file from message.
    # del xcom_msg_data[_CONTENTS_REF]
    return xcom_msg_data, filename
