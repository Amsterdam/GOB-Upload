"""
Populate a message with a hash

"""
import hashlib
import json

from gobcore.typesystem.json import GobTypeJSONEncoder


class Populator:

    def __init__(self, entity_model, msg):
        """
        Register the message attributes required for calculating the hash

        :param entity_model:
        :param msg:
        """
        self.id_column = entity_model["entity_id"]
        self.version = entity_model["version"]
        self.application = msg['header']['application']

    def populate(self, entity):
        """
        Populate an entity with a hash

        :param entity:
        :return:
        """
        entity["_id"] = entity[self.id_column]
        entity["_version"] = self.version
        entity['_hash'] = hashlib.md5((json.dumps(entity, sort_keys=True, cls=GobTypeJSONEncoder) +
                                       self.application).encode('utf-8')
                                      ).hexdigest()
