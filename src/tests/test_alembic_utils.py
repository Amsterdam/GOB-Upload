from unittest import TestCase

from gobupload.alembic_utils import get_query_split_json_column, get_query_merge_columns_to_jsonb_column


class TestAlembicUtils(TestCase):
    def test_get_query_split_json_column(self):
        result = get_query_split_json_column('nap_peilmerken', 'status', {'code': 'status_code', 'omschrijving': 'status_omschrijving'}, {'code': 'int', 'omschrijving': 'varchar'})
        expected = "UPDATE nap_peilmerken SET status_code=(status->>'code')::int, status_omschrijving=(status->>'omschrijving')::varchar"
        self.assertEquals(expected, result)

    def test_get_query_merge_columns_to_jsonb_column(self):
        result = get_query_merge_columns_to_jsonb_column('nap_peilmerken', 'merk', {'code': 'merk_code', 'omschrijving': 'merk_omschrijving'})
        expected = "UPDATE nap_peilmerken SET merk = jsonb_build_object('code', \"merk_code\", 'omschrijving', \"merk_omschrijving\")"
        self.assertEquals(expected, result)
        