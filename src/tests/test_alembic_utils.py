from unittest import TestCase
from unittest.mock import MagicMock, call

from gobupload.alembic_utils import RenamedRelation, get_query_split_json_column, \
    get_query_merge_columns_to_jsonb_column, upgrade_relations, downgrade_relations


class TestAlembicUtils(TestCase):
    def test_get_query_split_json_column(self):
        result = get_query_split_json_column('nap_peilmerken', 'status', {'code': 'status_code', 'omschrijving': 'status_omschrijving'}, {'code': 'int', 'omschrijving': 'varchar'})
        expected = "UPDATE nap_peilmerken SET status_code=(status->>'code')::int, status_omschrijving=(status->>'omschrijving')::varchar"
        self.assertEquals(expected, result)

    def test_get_query_merge_columns_to_jsonb_column(self):
        result = get_query_merge_columns_to_jsonb_column('nap_peilmerken', 'merk', {'code': 'merk_code', 'omschrijving': 'merk_omschrijving'})
        expected = "UPDATE nap_peilmerken SET merk = jsonb_build_object('code', \"merk_code\", 'omschrijving', \"merk_omschrijving\")"
        self.assertEquals(expected, result)

    renamed_relations = [
        RenamedRelation(
            table_name="table",
            old_column="old_relation_column",
            new_column="new_relation_column",
            old_relation_table="rel_old_relation_name",
            new_relation_table="rel_new_relation_name"
        )
    ]

    def test_upgrade_relations(self):
        op = MagicMock()
        upgrade_relations(op, self.renamed_relations)

        op.assert_has_calls([
            call.rename_table("rel_old_relation_name", "rel_new_relation_name"),
            call.alter_column("table", "old_relation_column", new_column_name="new_relation_column"),
            call.execute(
                "CREATE TABLE IF NOT EXISTS events.rel PARTITION OF events FOR VALUES IN ('rel') PARTITION BY LIST (entity)"),
            call.execute(
                "CREATE TABLE IF NOT EXISTS events.rel_new_relation_name PARTITION OF events.rel FOR VALUES IN ('new_relation_name') PARTITION BY LIST(source)"),
            call.execute(
                "CREATE TABLE IF NOT EXISTS events.rel_new_relation_name_gob PARTITION OF events.rel_new_relation_name FOR VALUES IN ('GOB')"),
            call.execute(
                "UPDATE events SET entity = 'new_relation_name' WHERE catalogue='rel' AND entity = 'old_relation_name'"),
        ])

    def test_downgrade_relations(self):
        op = MagicMock()
        downgrade_relations(op, self.renamed_relations)

        op.assert_has_calls([
            call.rename_table("rel_new_relation_name", "rel_old_relation_name"),
            call.alter_column("table", "new_relation_column", new_column_name="old_relation_column"),
            call.execute(
                "CREATE TABLE IF NOT EXISTS events.rel PARTITION OF events FOR VALUES IN ('rel') PARTITION BY LIST (entity)"),
            call.execute(
                "CREATE TABLE IF NOT EXISTS events.rel_old_relation_name PARTITION OF events.rel FOR VALUES IN ('old_relation_name') PARTITION BY LIST(source)"),
            call.execute(
                "CREATE TABLE IF NOT EXISTS events.rel_old_relation_name_gob PARTITION OF events.rel_old_relation_name FOR VALUES IN ('GOB')"),
            call.execute(
                "UPDATE events SET entity = 'old_relation_name' WHERE catalogue='rel' AND entity = 'new_relation_name'"),
        ])
