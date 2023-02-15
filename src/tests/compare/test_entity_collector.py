from unittest import TestCase
from unittest.mock import MagicMock

from gobupload.compare.entity_collector import EntityCollector
from gobupload.storage.handler import GOBStorageHandler


class TestEntityCollector(TestCase):

    def setUp(self):
        self.storage = MagicMock(spec=GOBStorageHandler)
        self.collector = EntityCollector(self.storage)
        self.collector._clear = MagicMock()

    def test_init(self):
        assert self.collector.storage == self.storage
        assert self.collector._entities == []

        with self.collector:
            self.storage.create_temporary_table.assert_called()
        self.storage.analyze_temporary_table.assert_called()

    def test_collect(self):
        entity = {"any": "value"}
        self.collector.collect(entity)

        assert self.collector._entities == [entity]
        self.storage.write_temporary_entities.assert_not_called()

        self.collector.CHUNKSIZE = 1
        self.collector.collect(entity)

        assert self.collector._entities == [entity] * 2
        self.storage.write_temporary_entities.assert_called_with(self.collector._entities)
        self.collector._clear.assert_called()

    def test_close(self):
        self.collector.close()

        # _entities empty -> write not called
        self.storage.write_temporary_entities.assert_not_called()
        self.storage.analyze_temporary_table.assert_called()

        self.collector._entities = [1, 2, 3, 4]
        self.collector.close()
        self.storage.write_temporary_entities.assert_called_with([1, 2, 3, 4])
        self.storage.analyze_temporary_table.assert_called()
