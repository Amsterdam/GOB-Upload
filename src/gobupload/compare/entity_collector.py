"""
EntityCollector

Stores each new entity in a temporary table
"""


class EntityCollector:

    def __init__(self, storage):
        """
        A storage is required to create the temporary table and write the entities to it
        :param storage:
        """
        self.storage = storage
        self._entities = []
        storage.create_temporary_table()

    def _write_entities(self):
        if self._entities:
            self.storage.write_temporary_entities(self._entities)
            self._entities.clear()

    def collect(self, entity):
        """
        Writes an entity to the temporary storage
        :param entity:
        :return:
        """
        self._entities.append(entity)

        if len(self._entities) >= 10_000:
            self._write_entities()

    def close(self):
        self._write_entities()
        self.storage.analyze_temporary_table()
