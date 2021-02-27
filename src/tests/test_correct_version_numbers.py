from unittest import TestCase

from unittest.mock import call, patch, MagicMock

from gobupload.correct_version_numbers import correct_version_numbers, _find_occurrence_of_column


class MockMigrations:
    _migrations = {
        'some_catalog': {
            'some_collection': {
                "0.1": {
                    "target_version": "0.2",
                    "conversions": [
                        {
                            "action": "delete",
                            "column": "deleted_column"
                        }
                    ]
                },
                "0.2": {
                    "target_version": "0.3",
                    "conversions": [
                        {
                            "action": "rename",
                            "old_column": "renamed_column_old_name",
                            "new_column": "renamed_column_new_name"
                        }
                    ]
                },
                "0.3": {
                    "target_version": "0.4",
                    "conversions": [
                        {
                            "action": "add",
                            "column": "added_column"
                        }
                    ]
                }
            }
        }
    }


def mock_find_occurrence(catalog_name, collection_name, column, first_or_last, storage):
    return {
        'deleted_column': {
            'first': 20,
            'last': 22,
        },
        'renamed_column_old_name': {
            'first': None,
            'last': None,
        },
        'renamed_column_new_name': {
            'first': 24,
            'last': 28,
        },
        'added_column': {
            'first': 30,
            'last': 30,
        }
    }[column][first_or_last]


class TestCorrectVersionNumbers(TestCase):

    def test_find_occurrence_of_column(self):

        storage = MagicMock()
        mock_execute = storage.get_session.return_value.__enter__.return_value.execute
        mock_execute.return_value = iter([(2490,)])

        self.assertEqual(2490, _find_occurrence_of_column('the cat', 'the coll', 'column_name', 'last', storage))
        expected_query = """
SELECT eventid FROM events
WHERE catalogue='the cat' AND entity='the coll' AND (
    (action='ADD' AND (contents->'entity')::jsonb ? 'column_name')
  OR
    (action='MODIFY' AND (contents->>'modifications')::jsonb @> '[{"key": "column_name"}]')
)
ORDER BY eventid DESC
LIMIT 1
"""
        mock_execute.assert_called_with(expected_query)

    @patch("gobupload.correct_version_numbers.GOBMigrations", MockMigrations)
    @patch("gobupload.correct_version_numbers._find_occurrence_of_column", mock_find_occurrence)
    @patch("gobupload.correct_version_numbers.GOBStorageHandler")
    def test_correct_version_numbers(self, mock_storage_handler):
        correct_version_numbers()

        mock_storage_handler.return_value.execute.assert_has_calls([
            call(
                "UPDATE events SET version='0.1' WHERE catalogue='some_catalog' AND entity='some_collection' AND eventid >= 0 AND eventid < 23"),
            call(
                "UPDATE events SET version='0.2' WHERE catalogue='some_catalog' AND entity='some_collection' AND eventid >= 23 AND eventid < 24"),
            call(
                "UPDATE events SET version='0.3' WHERE catalogue='some_catalog' AND entity='some_collection' AND eventid >= 24 AND eventid < 30"),
            call(
                "UPDATE events SET version='0.4' WHERE catalogue='some_catalog' AND entity='some_collection' AND eventid >= 30")
        ])
