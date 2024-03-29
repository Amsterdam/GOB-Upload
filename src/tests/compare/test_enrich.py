from unittest import TestCase
from unittest.mock import MagicMock, patch

from collections import namedtuple

from gobupload.storage.handler import GOBStorageHandler
from gobupload.compare.enrich import Enricher, _update_last_assigned, _autoid, AutoIdException


@patch('gobupload.compare.enrich.logger', MagicMock())
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
        enricher = Enricher(self.mock_storage, msg)
        for content in msg["contents"]:
            enricher.enrich(content)

        self.mock_storage.get_query_value.assert_not_called()
        self.assertEqual(msg["contents"], [])

    @patch("gobupload.compare.enrich.gob_model.has_states", lambda *args: False)
    def test_enrich_simple_contents(self):
        self.mock_storage.get_query_value.return_value = "POINT (1 2)"
        msg = self.mock_msg
        msg["contents"] = [
            {"x": [1, 2]}
        ]
        enricher = Enricher(self.mock_storage, msg)
        for content in msg["contents"]:
            enricher.enrich(content)

        qry = """
SELECT ST_AsText(ST_Union(geometrie))
FROM cat_col
JOIN (
            SELECT _tid
            FROM cat_col
            WHERE fld in (\'1\', \'2\')
        ) valid_tids
USING (_tid)
"""
        self.mock_storage.get_query_value.assert_called_with(qry)
        self.assertEqual(msg["contents"][0]["geo"], "POINT (1.000 2.000)")

    @patch("gobupload.compare.enrich.gob_model.has_states", lambda *args: False)
    def test_enrich_complex_contents(self):
        self.mock_storage.get_query_value.return_value = "POINT (1 2)"
        msg = self.mock_msg
        msg["header"]["enrich"]["geo"]["on"] = "x.y"
        msg["contents"] = [
            {"x": [{"y": 1}, {"y": 2}]}
        ]
        enricher = Enricher(self.mock_storage, msg)
        for content in msg["contents"]:
            enricher.enrich(content)

        qry = """
SELECT ST_AsText(ST_Union(geometrie))
FROM cat_col
JOIN (
            SELECT _tid
            FROM cat_col
            WHERE fld in (\'1\', \'2\')
        ) valid_tids
USING (_tid)
"""
        self.mock_storage.get_query_value.assert_called_with(qry)
        self.assertEqual(msg["contents"][0]["geo"], "POINT (1.000 2.000)")

    @patch("gobupload.compare.enrich.gob_model.has_states", lambda *args: False)
    def test_enrich_multi_complex_contents(self):
        self.mock_storage.get_query_value.return_value = "POINT (1 2)"
        msg = self.mock_msg
        msg["header"]["enrich"]["geo"]["on"] = "x.y.z"
        msg["contents"] = [
            {"x": [{"y": {"z": 1}}, {"y": {"z": 2}}]}
        ]
        enricher = Enricher(self.mock_storage, msg)
        for content in msg["contents"]:
            enricher.enrich(content)

        qry = """
SELECT ST_AsText(ST_Union(geometrie))
FROM cat_col
JOIN (
            SELECT _tid
            FROM cat_col
            WHERE fld in (\'1\', \'2\')
        ) valid_tids
USING (_tid)
"""
        self.mock_storage.get_query_value.assert_called_with(qry)
        self.assertEqual(msg["contents"][0]["geo"], "POINT (1.000 2.000)")

    def test_enrich_existing_contents(self):
        msg = self.mock_msg
        msg["contents"] = [
            {"geo": "aap"}
        ]
        enricher = Enricher(self.mock_storage, msg)
        for content in msg["contents"]:
            enricher.enrich(content)

        self.mock_storage.get_query_value.assert_not_called()
        self.assertEqual(msg["contents"][0]["geo"], "aap")

    @patch("gobupload.compare.enrich.gob_model.has_states", lambda *args: False)
    def test_enrich_geounion_none(self):
        self.mock_storage.get_query_value.return_value = None
        msg = self.mock_msg
        msg["contents"] = [{"x": [1, 2]}]
        enricher = Enricher(self.mock_storage, msg)

        for content in msg["contents"]:
            enricher.enrich(content)

        self.assertIsNone(msg["contents"][0]['geo'])

    @patch("gobupload.compare.enrich.gob_model.has_states", lambda *args: True)
    def test_enrich_states(self):
        self.mock_storage.get_query_value.return_value = "POINT (1 2)"
        msg = self.mock_msg
        msg["contents"] = [
            {"x": [1, 2]}
        ]
        enricher = Enricher(self.mock_storage, msg)
        for content in msg["contents"]:
            enricher.enrich(content)

        qry = """
SELECT ST_AsText(ST_Union(geometrie))
FROM cat_col
JOIN (
            SELECT _tid
            FROM (
                SELECT _tid, volgnummer, MAX(volgnummer) OVER (PARTITION BY fld) as max_volgnummer
                FROM cat_col
                WHERE fld in ('1', '2')
            ) tids
            WHERE volgnummer = max_volgnummer
        ) valid_tids
USING (_tid)
"""
        self.mock_storage.get_query_value.assert_called_with(qry)
        self.assertEqual(msg["contents"][0]["geo"], "POINT (1.000 2.000)")


@patch('gobupload.compare.enrich.logger', MagicMock())
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
        enricher = Enricher(self.mock_storage, msg)
        for content in msg["contents"]:
            enricher.enrich(content)

        self.assertEqual(msg["contents"], [])

    def test_enrich_non_empty_contents(self):
        msg = self.mock_msg
        msg["contents"] = [
            {"id": None, "code": "A"}
        ]
        self.mock_storage.get_column_values_for_key_value.return_value = None
        self.mock_storage.get_last_column_value.return_value = None
        enricher = Enricher(self.mock_storage, msg)
        for content in msg["contents"]:
            enricher.enrich(content)

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
        enricher = Enricher(self.mock_storage, msg)
        for content in msg["contents"]:
            enricher.enrich(content)

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
        enricher = Enricher(self.mock_storage, msg)
        for content in msg["contents"]:
            enricher.enrich(content)

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
            enricher = Enricher(self.mock_storage, msg)
            for content in msg["contents"]:
                enricher.enrich(content)

    def test_enrich_previously_assigned(self):
        """
        It can happen that in an import batch for some records an autoid had previously
        been assigned. That's why we always store the highest value in the database
        when the enricher has been initialized.
        """
        msg = self.mock_msg
        msg["header"]["enrich"]["id"]["template"] = "0123X"
        msg["contents"] = [
            {"id": None, "code": "0"},
            {"id": None, "code": "1"},
            {"id": None, "code": "2"}
        ]
        # Create a mock_record for the first entity in the message
        Record = namedtuple('Record', ['id', 'code'])
        mock_record = Record(id="01232", code="0")
        # The first record had been assigned an autoid in a previous run
        self.mock_storage.get_column_values_for_key_value.side_effect = [[mock_record], None, None]
        # The database contains records with higher values
        self.mock_storage.get_last_column_value.return_value = "1234"
        enricher = Enricher(self.mock_storage, msg)
        for content in msg["contents"]:
            enricher.enrich(content)

        self.assertEqual(msg["contents"][0]["id"], "01232")
        self.assertEqual(msg["contents"][1]["id"], "01235")
        self.assertEqual(msg["contents"][2]["id"], "01236")

    def test_enrich_id_already_filled(self):
        msg = self.mock_msg
        msg["contents"] = [
            {"id": "123", "code": "A"}
        ]
        self.mock_storage.get_column_values_for_key_value.return_value = None
        self.mock_storage.get_last_column_value.return_value = None
        enricher = Enricher(self.mock_storage, msg)
        for content in msg["contents"]:
            enricher.enrich(content)

        self.assertEqual(msg["contents"][0]["id"], "123")

    def test_enrich_with_last_value(self):
        msg = self.mock_msg
        msg["contents"] = [
            {"id": None, "code": "A"}
        ]
        self.mock_storage.get_column_values_for_key_value.return_value = None
        self.mock_storage.get_last_column_value.return_value = "123"
        enricher = Enricher(self.mock_storage, msg)
        for content in msg["contents"]:
            enricher.enrich(content)

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
        enricher = Enricher(self.mock_storage, msg)
        for content in msg["contents"]:
            enricher.enrich(content)

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
            enricher = Enricher(self.mock_storage, msg)
            for content in msg["contents"]:
                enricher.enrich(content)

    def test_enrich_dry_run(self):
        msg = self.mock_msg
        msg["contents"] = [
            {"id": None, "code": "0"},
        ]
        msg["header"]["enrich"]["id"]["dry_run"] = True

        self.mock_storage.get_column_values_for_key_value.return_value = None
        self.mock_storage.get_last_column_value.return_value = None
        enricher = Enricher(self.mock_storage, msg)

        for content in msg["contents"]:
            enricher.enrich(content)
            self.assertIsNone(content["id"])

    def test_update_last_assigned(self):
        column = 'any column'
        data = {
            column: 'ABC'
        }
        specs = {
            'template': '00XX'
        }
        assigned = {
            column: {
                'last': None
            }
        }
        _update_last_assigned(data, specs, column, assigned)
        self.assertEqual(assigned[column]['last'], None)

        data = {
            column: '1'
        }
        _update_last_assigned(data, specs, column, assigned)
        self.assertEqual(assigned[column]['last'], None)

        data = {
            column: '0024'
        }
        _update_last_assigned(data, specs, column, assigned)
        self.assertEqual(assigned[column]['last'], '0024')

    def test_autoid_conflict(self):
        storage = None
        column = 'any column'
        data = {
            column: 'ABC'
        }
        specs = {
            'on': 'any column',
            'template': '00XX'
        }
        assigned = {
            column: {
                'issued': {},
                'last': None
            }
        }
        result, _ = _autoid(storage, data, specs, column, assigned)
        self.assertEqual(result, 'ABC')

        assigned = {
            column: {
                'issued': {
                    'any id': 'ABC'
                },
                'last': None
            }
        }
        with self.assertRaises(AutoIdException):
            result, _ = _autoid(storage, data, specs, column, assigned)

    @patch("gobupload.compare.enrich._get_current_value")
    @patch("gobupload.compare.enrich._update_last_assigned")
    def test_autoid_with_current_value(self, mock_update, mock_current_value):
        storage = None
        column = 'any column'
        data = {
            column: None
        }
        specs = {
            'on': 'any column',
            'template': '00XX'
        }
        assigned = {
            column: {
                'issued': {},
                'last': None
            }
        }
        current_value = "any current value"
        mock_current_value.return_value = current_value
        result, _ = _autoid(storage, data, specs, column, assigned)
        self.assertEqual(result, current_value)
        self.assertEqual(data[column], current_value)
        mock_update.assert_called()
