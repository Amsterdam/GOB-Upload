import random
import string
import datetime

from collections import namedtuple

from gobcore.events import GOB_EVENTS, import_events, _get_event
from gobcore.events.import_message import MessageMetaData


def random_string(length=12, source=None):
    if source is None:
        source = string.ascii_letters

    return ''.join(random.choice(source) for x in range(length))


def random_array(callable=random_string):
    result = []
    for i in range(0, random.randint(1, 4)):
        result.append(callable())
    return result


def random_dict(callable=random_string):
    return {key: callable for key in random_array(random_string)}


# todo: these fixtures should have a tighter relation to the objects in GOB-Core
def get_message_fixture(contents=None, **kwargs):
    header = get_metadata_fixture()

    if contents is None:
        contents = [get_data_object(header, **kwargs)]

    return {'header': header,
            'summary': None,
            'contents': contents}


def random_gob_event():
    return random.choice(GOB_EVENTS)


def get_event_data_fixture(gob_event, metadata):
    if gob_event.name == 'MODIFY':
        return {**get_data_object(metadata), **{import_events.modifications_key: {}}}

    return get_data_object(metadata)


def get_event_fixture(metadata, event_name=None):
    gob_event = random_gob_event() if event_name is None else _get_event(event_name)
    data = get_event_data_fixture(gob_event, metadata)
    data["_last_event"] = None
    data["_hash"] = None
    return gob_event.create_event(data["_source_id"], "", data)


def get_metadata_fixture():
    header = {key: random_string() for key in ["source", "timestamp", "version", "application"]}
    header["catalogue"] = "meetbouten"
    header["entity"] = "meetbouten"
    header["id_column"] = "identificatie"
    header["model"] = {header['id_column']: {"type": "GOB.String"}}
    header["process_id"] = f"{header['timestamp']}.{header['source']}.{header['entity']}"
    return header


def get_entity_fixture(**kwargs):
    class Object(object):
        pass

    entity = Object()
    for attr, value in kwargs.items():
        setattr(entity, attr, value)
    setattr(entity, "_last_event", None)
    return entity


def get_data_object(metadata, **kwargs):
    data_object = {'_source_id': random_string(), metadata["id_column"]: random_string()}
    for field, value in kwargs.items():
        metadata["model"][field] = {"type": "GOB.String"}
        data_object[field] = str(value)

    return data_object


def get_event_message_fixture(event_name=None):
    message = get_message_fixture()
    metadata = message['header']
    event = get_event_fixture(metadata, event_name)
    message['contents'] = {
        "events": [event],
        "recompares": []
    }

    for field in event['data'].keys():
        if field != '_source_id':
            metadata["model"][field] = {"type": "GOB.String"}
    message['header'] = metadata

    return message


def dict_to_object(dict):
    class Obj(object):
        def __init__(self, dict):
            self.__dict__ = dict
    return Obj(dict)


def get_event_fixure():
    event = {
        'version': '0.1',
        'catalogue': 'test_catalogue',
        'application': 'TEST',
        'entity': 'test_entity',
        'timestamp': datetime.datetime(2019, 1, 30, 18, 7, 7),
        'source': 'test',
        'action': None,
    }

    return dict_to_object(event)
