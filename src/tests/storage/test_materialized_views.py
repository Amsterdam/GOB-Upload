import re

from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobupload.storage.materialized_views import MaterializedView, MaterializedViews


class MockModel:
    data = {
        'cata': {
            'cola': {
                'has_states': True,
            }
        },
        'catb': {
            'colb': {
                'has_states': False,
            }
        }
    }

    def get_catalog_collection_from_abbr(self, cat, col):
        return self.data[cat], self.data[cat][col]


class TestMaterializedView(TestCase):

    @patch("gobupload.storage.materialized_views.MaterializedView.model", MockModel())
    def setUp(self):
        self.relation_name = 'cata_cola_catb_colb_some_reference_field'
        self.mv = MaterializedView(self.relation_name)

    def test_init(self):
        self.assertEqual(self.relation_name, self.mv.relation_name)
        self.assertEqual(f"mv_{self.relation_name}", self.mv.name)
        self.assertEqual(f"rel_{self.relation_name}", self.mv.relation_table_name)
        self.assertTrue(self.mv.src_has_states)
        self.assertFalse(self.mv.dst_has_states)
        self.assertTrue('some_reference_field', self.mv.attribute_name)

    def test_refresh(self):
        storage_handler = MagicMock()
        self.mv.refresh(storage_handler)
        storage_handler.execute.assert_called_with(f"REFRESH MATERIALIZED VIEW {self.mv.name}")

    def test_create(self):
        self.mv._create_indexes = MagicMock()
        storage_handler = MagicMock()

        self.mv.create(storage_handler, False)
        storage_handler.execute.assert_called_with(
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS {self.mv.name} AS "
            f"SELECT _gobid,src_id,src_volgnummer,dst_id,begin_geldigheid,eind_geldigheid,bronwaarde "
            f"FROM {self.mv.relation_table_name} WHERE _date_deleted IS NULL")

        self.mv._create_indexes.assert_called_with(storage_handler, False)

        combinations = [
            (True, True, '_gobid,src_id,src_volgnummer,dst_id,dst_volgnummer,begin_geldigheid,eind_geldigheid,bronwaarde'),
            (True, False, '_gobid,src_id,src_volgnummer,dst_id,begin_geldigheid,eind_geldigheid,bronwaarde'),
            (False, True, '_gobid,src_id,dst_id,dst_volgnummer,begin_geldigheid,eind_geldigheid,bronwaarde'),
            (False, False, '_gobid,src_id,dst_id,begin_geldigheid,eind_geldigheid,bronwaarde'),
        ]

        pattern = re.compile(r'AS SELECT ([a-z_,]+) FROM')
        for src_has_states, dst_has_states, columns in combinations:
            self.mv.src_has_states = src_has_states
            self.mv.dst_has_states = dst_has_states

            self.mv.create(storage_handler)
            query = storage_handler.execute.call_args[0][0]
            matches = pattern.search(query)

            # Assert the correct columns are included in the materialized view
            self.assertEqual(columns, matches[1])

    def test_create_force_recreate(self):
        self.mv._create_indexes = MagicMock()
        storage_handler = MagicMock()

        self.mv.create(storage_handler, True)
        storage_handler.execute.assert_has_calls([
            call(f"DROP MATERIALIZED VIEW IF EXISTS {self.mv.name} CASCADE"),
            call(f"CREATE MATERIALIZED VIEW IF NOT EXISTS {self.mv.name} AS "
                f"SELECT _gobid,src_id,src_volgnummer,dst_id,begin_geldigheid,eind_geldigheid,bronwaarde "
                f"FROM {self.mv.relation_table_name} WHERE _date_deleted IS NULL"),
        ])

        self.mv._create_indexes.assert_called_with(storage_handler, True)

    def test_create_indexes(self):
        storage_handler = MagicMock()
        self.mv.dst_has_states = True
        self.mv.src_has_states = True
        self.mv._create_indexes(storage_handler)

        storage_handler.execute.assert_has_calls([
            call(f"CREATE INDEX IF NOT EXISTS src_id_{self.mv.name} ON {self.mv.name}(src_id)"),
            call(f"CREATE INDEX IF NOT EXISTS dst_id_{self.mv.name} ON {self.mv.name}(dst_id)"),
            call(f"CREATE INDEX IF NOT EXISTS gobid_{self.mv.name} ON {self.mv.name}(_gobid)"),
            call(f"CREATE INDEX IF NOT EXISTS src_dst_wide_{self.mv.name} ON "
                 f"{self.mv.name}(src_id,src_volgnummer,dst_id,dst_volgnummer)")
        ])


@patch("gobupload.storage.materialized_views.GOBModel", MagicMock())
class TestMaterializedViews(TestCase):

    def test_initialise(self):
        mv = MaterializedViews()
        storage_handler = MagicMock()
        mocked_view = MagicMock()

        mv.get_all = MagicMock(return_value=[mocked_view])
        mv.initialise(storage_handler)
        mocked_view.create.assert_called_with(storage_handler, False)

        mv.initialise(storage_handler, True)
        mocked_view.create.assert_called_with(storage_handler, True)

    @patch("gobupload.storage.materialized_views.model_relations.get_relations")
    @patch("gobupload.storage.materialized_views.MaterializedView")
    def test_get_all(self, mock_materialized_view, mock_get_relations):
        mock_get_relations.return_value = {
            'collections': {
                'rel_name_a': {},
                'rel_name_b': {},
            }
        }

        mock_materialized_view.side_effect = lambda x: 'mv_' + x

        mv = MaterializedViews()
        result = mv.get_all()

        self.assertEqual(['mv_rel_name_a', 'mv_rel_name_b'], result)
        mock_materialized_view.assert_has_calls([
            call('rel_name_a'),
            call('rel_name_b'),
        ])

    @patch("gobupload.storage.materialized_views.model_relations.get_relation_name",
           lambda m, cat, col, attr: f"{cat}_{col}_{attr}")
    @patch("gobupload.storage.materialized_views.MaterializedView", lambda x: 'mv_' + x)
    def test_get(self):
        mv = MaterializedViews()
        res = mv.get('cat', 'col', 'attr')

        self.assertEqual('mv_cat_col_attr', res)
