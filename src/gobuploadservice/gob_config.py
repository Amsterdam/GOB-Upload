GOB_TIMESTAMPS = {
    "ADD": '_date_created',
    "MODIFIED": '_date_modified',
    "DELETED": '_date_deleted',
    "CONFIRMED": '_date_confirmed',
}
GOB_ACTIONS = ["ADD", "DELETED", "MODIFIED", "CONFIRMED"]


class GOBHeader():
    source_id_column = '_source_id'

    def __init__(self, msg):
        self._header = msg["header"]
        assert self.source is not None
        assert self.timestamp is not None
        assert self.id_column is not None
        assert self.entity is not None
        assert self.version is not None

    @property
    def source(self):
        return self._header['source']

    @property
    def timestamp(self):
        return self._header['timestamp']

    @property
    def id_column(self):
        return self._header['entity_id']

    @property
    def entity(self):
        return self._header['entity']

    @property
    def version(self):
        return self._header['version']

    @property
    def as_header(self):
        return {
            "source": self.source,
            "timestamp": self.timestamp,
            "entity_id": self.id_column,
            "entity": self.entity,
            "version": self.version
        }
