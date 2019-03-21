from unittest import TestCase
from unittest.mock import MagicMock, patch

from collections import namedtuple

from gobupload.storage.handler import GOBStorageHandler
from gobupload.enrich import enrich, _autoid


@patch('gobupload.enrich.logger', MagicMock())
class TestEnrichGeounion(TestCase):
    def setUp(self):
        self.mock_storage = MagicMock(spec=GOBStorageHandler)
        self.mock_msg = {
            "header": {
                "enrich": {
                    "geo": {
                        "type": "geounion",
                        "on": "x",
                        "from": "cat:col:fld",
                        "geometrie": "geometrie"
                    }
                }
            },
            "contents": []
        }

    def tearDown(self):
        pass

    def test_enrich_empty_contents(self):
        msg = self.mock_msg
        msg["contents"] = []
        enrich(self.mock_storage, msg)

        self.mock_storage.get_query_value.assert_not_called()
        self.assertEqual(msg["contents"], [])

    def test_enrich_simple_contents(self):
        self.mock_storage.get_query_value.return_value = "POINT (1 2)"
        msg = self.mock_msg
        msg["contents"] = [
            {"x": [1, 2]}
        ]
        enrich(self.mock_storage, msg)

        self.mock_storage.get_query_value.assert_called_with("""
SELECT
      ST_AsText(
          ST_Union(geometrie)
      )
FROM  cat_col
WHERE fld in ('1', '2')
AND   eind_geldigheid IS NULL
""")
        self.assertEqual(msg["contents"][0]["geo"], "POINT (1.000 2.000)")

    def test_enrich_complex_contents(self):
        self.mock_storage.get_query_value.return_value = "POINT (1 2)"
        msg = self.mock_msg
        msg["header"]["enrich"]["geo"]["on"] = "x.y"
        msg["contents"] = [
            {"x": [{"y": 1}, {"y": 2}]}
        ]
        enrich(self.mock_storage, msg)

        self.mock_storage.get_query_value.assert_called_with("""
SELECT
      ST_AsText(
          ST_Union(geometrie)
      )
FROM  cat_col
WHERE fld in ('1', '2')
AND   eind_geldigheid IS NULL
""")
        self.assertEqual(msg["contents"][0]["geo"], "POINT (1.000 2.000)")

    def test_enrich_mulit_complex_contents(self):
        self.mock_storage.get_query_value.return_value = "POINT (1 2)"
        msg = self.mock_msg
        msg["header"]["enrich"]["geo"]["on"] = "x.y.z"
        msg["contents"] = [
            {"x": [{"y": {"z": 1}}, {"y": {"z": 2}}]}
        ]
        enrich(self.mock_storage, msg)

        self.mock_storage.get_query_value.assert_called_with("""
SELECT
      ST_AsText(
          ST_Union(geometrie)
      )
FROM  cat_col
WHERE fld in ('1', '2')
AND   eind_geldigheid IS NULL
""")
        self.assertEqual(msg["contents"][0]["geo"], "POINT (1.000 2.000)")

    def test_enrich_existing_contents(self):
        msg = self.mock_msg
        msg["contents"] = [
            {"geo": "aap"}
        ]
        enrich(self.mock_storage, msg)

        self.mock_storage.get_query_value.assert_not_called()
        self.assertEqual(msg["contents"][0]["geo"], "aap")


@patch('gobupload.enrich.logger', MagicMock())
class TestEnrichAutoid(TestCase):

    def setUp(self):
        self.mock_storage = MagicMock(spec=GOBStorageHandler)
        self.mock_msg = {
            "header": {
                "enrich": {
                    "id": {
                        "type": "autoid",
                        "on": "code",
                        "template": "0123X"
                    }
                }
            },
            "contents": []
        }

    def tearDown(self):
        pass

    def test_enrich_empty_contents(self):
        msg = self.mock_msg
        msg["contents"] = []
        self.mock_storage.get_column_values_for_key_value.return_value = None
        self.mock_storage.get_last_column_value.return_value = None
        enrich(self.mock_storage, msg)

        self.assertEqual(msg["contents"], [])

    def test_enrich_non_empty_contents(self):
        msg = self.mock_msg
        msg["contents"] = [
            {"id": None, "code": "A"}
        ]
        self.mock_storage.get_column_values_for_key_value.return_value = None
        self.mock_storage.get_last_column_value.return_value = None
        enrich(self.mock_storage, msg)

        self.assertEqual(msg["contents"][0]["id"], "01230")

    def test_enrich_reuse_value(self):
        msg = self.mock_msg
        msg["contents"] = [
            {"id": None, "code": "A"},
            {"id": None, "code": "B"},
            {"id": None, "code": "A"},
            {"id": None, "code": "B"},
        ]
        self.mock_storage.get_column_values_for_key_value.return_value = None
        self.mock_storage.get_last_column_value.return_value = None
        enrich(self.mock_storage, msg)

        self.assertEqual(msg["contents"][0]["id"], "01230")
        self.assertEqual(msg["contents"][1]["id"], "01231")
        self.assertEqual(msg["contents"][2]["id"], "01230")
        self.assertEqual(msg["contents"][3]["id"], "01231")

    def test_enrich_max_contents(self):
        msg = self.mock_msg
        msg["contents"] = [
            {"id": None, "code": "0"},
            {"id": None, "code": "1"},
            {"id": None, "code": "2"},
            {"id": None, "code": "3"},
            {"id": None, "code": "4"},
            {"id": None, "code": "5"},
            {"id": None, "code": "6"},
            {"id": None, "code": "7"},
            {"id": None, "code": "8"},
            {"id": None, "code": "9"},
        ]
        self.mock_storage.get_column_values_for_key_value.return_value = None
        self.mock_storage.get_last_column_value.return_value = None
        enrich(self.mock_storage, msg)

        self.assertEqual(msg["contents"][0]["id"], "01230")
        self.assertEqual(msg["contents"][9]["id"], "01239")

    def test_enrich_overflow_contents(self):
        msg = self.mock_msg
        msg["contents"] = [
            {"id": None, "code": "0"},
            {"id": None, "code": "1"},
            {"id": None, "code": "2"},
            {"id": None, "code": "3"},
            {"id": None, "code": "4"},
            {"id": None, "code": "5"},
            {"id": None, "code": "6"},
            {"id": None, "code": "7"},
            {"id": None, "code": "8"},
            {"id": None, "code": "9"},
            {"id": None, "code": "A"}
        ]
        self.mock_storage.get_column_values_for_key_value.return_value = None
        self.mock_storage.get_last_column_value.return_value = None

        with self.assertRaises(AssertionError):
            enrich(self.mock_storage, msg)

    def test_enrich_id_already_filled(self):
        msg = self.mock_msg
        msg["contents"] = [
            {"id": "123", "code": "A"}
        ]
        self.mock_storage.get_column_values_for_key_value.return_value = None
        self.mock_storage.get_last_column_value.return_value = None
        enrich(self.mock_storage, msg)

        self.assertEqual(msg["contents"][0]["id"], "123")

    def test_enrich_with_last_value(self):
        msg = self.mock_msg
        msg["contents"] = [
            {"id": None, "code": "A"}
        ]
        self.mock_storage.get_column_values_for_key_value.return_value = None
        self.mock_storage.get_last_column_value.return_value = "123"
        enrich(self.mock_storage, msg)

        # Check that the length is OK (padded with zeroes) and that 1 is added (123 => 124)
        self.assertEqual(msg["contents"][0]["id"], "00124")

    def test_enrich_with_current_value(self):
        msg = self.mock_msg
        msg["contents"] = [
            {"id": None, "code": "A"}
        ]
        Record = namedtuple('Record', ['id', 'code'])
        self.mock_storage.get_column_values_for_key_value.return_value = [
            Record(id="123", code="A")
        ]
        self.mock_storage.get_last_column_value.return_value = None
        enrich(self.mock_storage, msg)

        # Check that the length is OK (padded with zeroes) and that 1 is added (123 => 124)
        self.assertEqual(msg["contents"][0]["id"], "123")

    def test_enrich_with_mulitple_current_values(self):
        msg = self.mock_msg
        msg["contents"] = [
            {"id": None, "code": "A"}
        ]
        Record = namedtuple('Record', ['id', 'code'])
        self.mock_storage.get_column_values_for_key_value.return_value = [
            Record(id="123", code="A"),
            Record(id="456", code="A"),
        ]
        self.mock_storage.get_last_column_value.return_value = None
        with self.assertRaises(AssertionError):
            enrich(self.mock_storage, msg)

    def test_autoid(self):
        pass
