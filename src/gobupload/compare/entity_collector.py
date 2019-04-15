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
        self.storage.create_temporary_table()

    def close(self):
        self.storage.close_temporary_table()

    def collect(self, entity):
        """
        Writes an entity to the temporary storage
        :param entity:
        :return:
        """
        self.storage.write_temporary_entity(entity)
