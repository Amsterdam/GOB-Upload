class EntityCollector:

    def __init__(self, storage):
        self.storage = storage

    def __enter__(self):
        self.storage.create_temporary_table()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.storage.close_temporary_table()

    def collect(self, entity):
        self.storage.write_temporary_entity(entity)
