import unittest

from collections import namedtuple

from gobupload.storage.db_models.event import build_db_event


class TestBuildEvent(unittest.TestCase):

    def test_build_db_event(self):
        metadata = namedtuple('Meta', ['timestamp', 'catalogue', 'entity', 'version', 'source', 'application'])
        event = {
            "data": {
                "_source_id": None
            },
            "event": None
        }
        builder = lambda **kwargs: ".".join(kwargs.keys())
        build = build_db_event(builder, event, metadata)
        self.assertEqual(build, 'timestamp.catalogue.entity.version.action.source.application.source_id.contents')
