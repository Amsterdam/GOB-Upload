"""
EntityCollector

Stores each new entity in a temporary table
"""


class EntityCollector:

    CHUNKSIZE = 10_000

    def __init__(self, storage):
        """
        A storage is required to create the temporary table and write the entities to it
        :param storage:
        """
        self.storage = storage
        self._entities = []

    def __enter__(self):
        self.storage.create_temporary_table()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _clear(self):
        self._entities.clear()  # pragma: no cover

    def _write_entities(self):
        if self._entities:
            self.storage.write_temporary_entities(self._entities)
            self._clear()

    def collect(self, entity):
        """
        Writes an entity to the temporary storage
        :param entity:
        :return:
        """
        self._entities.append(entity)

        if len(self._entities) >= self.CHUNKSIZE:
            self._write_entities()

    def close(self):
        self._write_entities()
        self.storage.analyze_temporary_table()
