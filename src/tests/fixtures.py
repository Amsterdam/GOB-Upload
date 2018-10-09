import random
import string

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
        metadata = MessageMetaData(**header)
        contents = [get_data_object(metadata, **kwargs)]
        header = metadata.as_header

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
    entity_id = data.pop(metadata.id_column)
    return gob_event.create_event(data[metadata.source_id_column], metadata.id_column, entity_id, data)


def get_metadata_fixture():
    header = {key: random_string() for key in ["source", "timestamp", "version"]}
    header["entity"] = "meetbouten"
    header["id_column"] = "meetboutid"
    header["model"] = {header['id_column']: {"type": "GOB.String"}}
    header["process_id"] = f"{header['timestamp']}.{header['source']}.{header['entity']}"
    return MessageMetaData(**header).as_header


def get_entity_fixture(**kwargs):
    class Object(object):
        pass

    entity = Object()
    for attr, value in kwargs.items():
        setattr(entity, attr, value)
    return entity


def get_data_object(metadata, **kwargs):
    data_object = {'_source_id': random_string(), metadata.id_column: random_string()}
    for field, value in kwargs.items():
        metadata.model[field] = {"type": "GOB.String"}
        data_object[field] = str(value)

    return data_object


def get_event_message_fixture(event_name=None):
    message = get_message_fixture()
    metadata = MessageMetaData(**message['header'])
    event = get_event_fixture(metadata, event_name)
    message['contents'] = [event]

    for field in event['data'].keys():
        if field != '_source_id':
            metadata.model[field] = {"type": "GOB.String"}
    message['header'] = metadata.as_header

    return message
