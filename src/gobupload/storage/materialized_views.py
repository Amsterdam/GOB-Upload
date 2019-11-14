"""
Contains the logic for handling the materialized views on the relation tables

To prevent pulling complete rows into memory when querying relations through the relation tables, this class
defines materialized views containing only the minimal set of of columns of the relation table to be able to query
on relations.

The name of the materialized view is equal to the name of the relation table, except that the prefix rel_ is
replaced with the prefix mv_, for example:

rel_brk_kot_brk_gme_aangeduid_door_gemeente gets a materialized view named
mv_brk_kot_brk_gme_aangeduid_door_gemeente

The columns included in the materialized views:
src_id
src_volgnummer (only if src has states)
dst_id
dst_volgnummer (only if dst has states)
bronwaarde

Also, two indexes are created on the materialized views: One on the src_id column, the other on the dst_id column.

Initialisation:
mv = MaterializedViews()
mv.initialise(storage_handler)

Update materialized view for catalog, collection, attribute
"""
from gobcore.model.metadata import FIELD
from gobcore.model import GOBModel
from gobcore.model import relations as model_relations


class MaterializedView:
    model = GOBModel()

    def __init__(self, relation_name: str):
        """

        :param relation_name: relation name of the form brk_kot_brk_gme_aangeduid_door_gemeente
        """
        self.relation_name = relation_name
        self.name = f"mv_{relation_name}"
        self.relation_table_name = f"rel_{relation_name}"

        split = relation_name.split('_')
        src_catalog, src_collection = self.model.get_catalog_collection_from_abbr(split[0], split[1])
        dst_catalog, dst_collection = self.model.get_catalog_collection_from_abbr(split[2], split[3])

        self.src_has_states = src_collection.get('has_states', False)
        self.dst_has_states = dst_collection.get('has_states', False)

        self.attribute_name = split[4:]

    def refresh(self, storage_handler):
        query = f"REFRESH MATERIALIZED VIEW {self.name}"
        storage_handler.execute(query)

    def create(self, storage_handler, force_recreate=False):
        include_columns = {
            FIELD.GOBID: True,
            f"src{FIELD.ID}": True,
            f"src_{FIELD.SEQNR}": self.src_has_states,
            f"dst{FIELD.ID}": True,
            f"dst_{FIELD.SEQNR}": self.dst_has_states,
            FIELD.SOURCE_VALUE: True,
        }

        fields = ','.join([field for field, include in include_columns.items() if include])

        if force_recreate:
            storage_handler.execute(f"DROP MATERIALIZED VIEW IF EXISTS {self.name} CASCADE")

        query = \
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS {self.name} AS SELECT {fields} " \
            f"FROM {self.relation_table_name} WHERE {FIELD.DATE_DELETED} IS NULL"
        storage_handler.execute(query)

        self._create_indexes(storage_handler, force_recreate)

    def _create_indexes(self, storage_handler, force_recreate=False):
        indexes = {
            f"src_id_{self.name}": [f"src{FIELD.ID}"],
            f"dst_id_{self.name}": [f"dst{FIELD.ID}"],
            f"gobid_{self.name}": [FIELD.GOBID],
        }

        wide_index = {
            f"src{FIELD.ID}": True,
            f"src_{FIELD.SEQNR}": self.src_has_states,
            f"dst{FIELD.ID}": True,
            f"dst_{FIELD.SEQNR}": self.dst_has_states,
        }
        indexes[f"src_dst_wide_{self.name}"] = [field for field, include in wide_index.items() if include]

        for index_name, columns in indexes.items():
            if force_recreate:
                storage_handler.execute(f"DROP INDEX IF EXISTS {index_name}")

            # Temporary disable
            # query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {self.name}({','.join(columns)})"
            # storage_handler.execute(query)


class MaterializedViews:
    model = GOBModel()

    def initialise(self, storage_handler, force_recreate=False):
        """This method creates the materialized view along with its indexes

        :return:
        """
        materialized_views = self.get_all()

        for materialized_view in materialized_views:
            materialized_view.create(storage_handler, force_recreate)

    def get_all(self):
        """Returns definitions of materialized views

        :return:
        """
        definitions = []

        for relation_name in model_relations.get_relations(self.model)['collections'].keys():
            definitions.append(MaterializedView(relation_name))

        return definitions

    def get(self, catalog_name, collection_name, attribute):
        """Returns definition of materialized view for given relation.

        :param collection_name:
        :param catalog_name:
        :param attribute:
        :return:
        """
        relation_name = model_relations.get_relation_name(self.model, catalog_name, collection_name, attribute)
        return MaterializedView(relation_name)
