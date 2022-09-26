"""Populate a message with a hash."""

import hashlib
import json

from gobcore.typesystem.json import GobTypeJSONEncoder
from gobcore.model.metadata import FIELD


class Populator:

    def __init__(self, entity_model, msg):
        """
        Register the message attributes required for calculating the hash

        :param entity_model:
        :param msg:
        """
        self.id_column = entity_model["entity_id"]
        self.version = entity_model["version"]
        self.has_states = entity_model.get("has_states", False)
        self.application = msg['header']['application']

    def populate(self, entity):
        """
        Populate an entity with a hash

        :param entity:
        :return:
        """
        entity[FIELD.ID] = entity[self.id_column]
        entity[FIELD.VERSION] = self.version
        entity[FIELD.HASH] = hashlib.md5((json.dumps(entity, sort_keys=True, cls=GobTypeJSONEncoder) +
                                          self.application).encode('utf-8')
                                         ).hexdigest()
        entity[FIELD.TID] = f"{entity[FIELD.ID]}.{entity[FIELD.SEQNR]}" if self.has_states else entity[FIELD.ID]
