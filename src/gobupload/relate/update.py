"""See README.md in this directory for explanation of this file."""

import hashlib
import json
from datetime import date, datetime

from gobcore.events.import_events import ADD, CONFIRM, DELETE, MODIFY
from gobcore.exceptions import GOBException
from gobcore.logging.logger import logger
from gobcore.message_broker.offline_contents import ContentsWriter
from gobcore.model.metadata import FIELD
from gobcore.model.relations import get_relation_name
from gobcore.sources import GOBSources
from gobcore.typesystem.json import GobTypeJSONEncoder
from gobcore.utils import ProgressTicker

from gobupload import gob_model
from gobupload.compare.event_collector import EventCollector
from gobupload.config import DEBUG
from gobupload.storage.execute import _execute
from gobupload.utils import random_string
from gobupload.relate.exceptions import RelateException

EQUALS = 'equals'
LIES_IN = 'lies_in'
GOB = 'GOB'

# Maximum number of error messages to report
_MAX_RELATION_CONFLICTS = 25

# Ratio of changed src_objects + changed dst_objects that trigger a full relate.
# Value is between 0 and 2. 0 is full relate always. 2 is full relate when everything is changed.
# 1.0 means that 50% of src objects and 50% of dst objects have changed (0.5 + 0.5), or 100% and 0%, or 60/40
_FORCE_FULL_RELATE_THRESHOLD = 1.0

# Used for initial relate. 30000 seems to be the sweetspot for the current configuration
_MAX_ROWS_PER_SIDE = 30000

_RELATE_VERSION = '0.1'


# DEVELOPMENT: The DEBUG environment to 'true' to avoid removal of tmp tables.

class StartValiditiesTable:
    """Holds the start validities table for a given object.

    For objects with state (volgnummers), for a given state, the begin_geldigheid that is used in the relate process
    is the begin_geldigheid of the first state of the set of consecutive states this state is a member of.

    For example, we have a certain object A with volgnummers 1, 2, 3, 5, 6 and 8

    We have three sets of consecutive states:
    1, 2, 3
    5, 6
    8

    Each state in each series will get the begin_geldigheid of the first state in its set. This means that states 2
    and 3 get the begin_geldigheid of state 1. 6 uses the begin_geldigheid of 5 and 1, 5 and 8 don't change.

    These new values for begin_geldigheid are used only to determine the begin_geldigheid for the relation.

    The resulting table contains the columns id, volgnummer, begin_geldigheid
    """

    def __init__(self, from_table: str, name: str):
        """

        :param from_table: The table to extract the calculate the start validities for
        :param name: Name of the table to be created
        """
        self.from_table = from_table
        self.name = name

    def _query(self):
        """Returns the query that builds the table

        :return:
        """
        return f"""
-- Find all possible intervals for {self.from_table}: id - seqnr - start - end, using windows per `_id`
-- 1. lag (sorted) end_validity 1 row to allow for comparison with start_validity of next period
-- 2. create column with labels of consecutive periods (when there is a gap, start a new group)
-- 3. Take the first start_validity per group
WITH
end_validity_lag AS (
    SELECT {FIELD.ID}, {FIELD.SEQNR}, {FIELD.START_VALIDITY}, LAG({FIELD.END_VALIDITY}) OVER w AS lagged_end
    FROM {self.from_table}
    WINDOW w AS (PARTITION BY {FIELD.ID} ORDER BY {FIELD.ID}, {FIELD.SEQNR})
),
group_start_validity AS (
    SELECT {FIELD.ID}, {FIELD.SEQNR}, {FIELD.START_VALIDITY},
        COUNT(*) FILTER ( WHERE {FIELD.START_VALIDITY} <> lagged_end ) OVER w AS consecutive_period
    FROM end_validity_lag
    WINDOW w AS (PARTITION BY {FIELD.ID} ORDER BY {FIELD.ID}, {FIELD.SEQNR})
)
SELECT {FIELD.ID}, {FIELD.SEQNR}, MIN({FIELD.START_VALIDITY}) OVER w AS {FIELD.START_VALIDITY}
FROM group_start_validity
WINDOW w AS (PARTITION BY {FIELD.ID}, consecutive_period ORDER BY {FIELD.ID}, {FIELD.SEQNR})
"""

    def create(self):
        """Creates the table with index on id, volgnummer

        :return:
        """
        self.drop()
        query = self._query()

        _execute(f"CREATE UNLOGGED TABLE {self.name} AS ({query})")
        _execute(f"CREATE INDEX ON {self.name}({FIELD.ID}, {FIELD.SEQNR})")

    def drop(self):
        """Drops this table

        :return:
        """
        _execute(f"DROP TABLE IF EXISTS {self.name}")

    @classmethod
    def from_catalog_collection(cls, catalog_name: str, collection_name: str, table_name: str):
        from_table = gob_model.get_table_name(catalog_name, collection_name)
        return cls(from_table, table_name)


class EventCreator:

    def __init__(self, dst_has_states: bool):
        self.dst_has_states = dst_has_states

    def _get_modifications(self, row: dict, compare_fields: list):
        modifications = []

        for field in compare_fields:
            old_value = row[f"rel_{field}"]
            new_value = row[field]

            if old_value != new_value:
                modifications.append({
                    'old_value': old_value,
                    'new_value': new_value,
                    'key': field,
                })

        return modifications

    def _get_hash(self, row: dict):
        return hashlib.md5(
            (json.dumps(row, sort_keys=True, cls=GobTypeJSONEncoder) + row[FIELD.APPLICATION]).encode('utf-8')
        ).hexdigest()

    def create_event(self, row: dict):
        compare_fields = [
            'dst_id',
            FIELD.EXPIRATION_DATE,
            FIELD.START_VALIDITY,
            FIELD.END_VALIDITY,
        ]

        if self.dst_has_states:
            compare_fields.append('dst_volgnummer')

        # FIELD.ID, rel_id are equal to the TID
        if row['rel_id'] is None or row['rel_deleted'] is not None:
            # No relation yet, or previously deleted relation. Create ADD
            ignore_fields = [
                                'src_deleted',
                                'rel_deleted',
                                'src_last_event',
                                'rel_id',
                                f'rel_{FIELD.HASH}',
                                'rel_dst_volgnummer',
                                'rowid',
                            ] + [f"rel_{field}" for field in compare_fields]

            data = {k: v for k, v in row.items() if k not in ignore_fields}
            return ADD.create_event(row[FIELD.ID], data, _RELATE_VERSION)

        if row['src_deleted'] is not None or row['src_id'] is None:
            # src id marked as deleted or doesn't exist
            data = {FIELD.LAST_EVENT: row[FIELD.LAST_EVENT]}
            return DELETE.create_event(row['rel_id'], data, _RELATE_VERSION)

        row[FIELD.HASH] = self._get_hash(row)
        modifications = [] \
            if row[FIELD.HASH] == row[f"rel_{FIELD.HASH}"] \
            else self._get_modifications(row, compare_fields)

        if modifications:
            data = {
                'modifications': modifications,
                FIELD.LAST_EVENT: row[FIELD.LAST_EVENT],
                FIELD.HASH: row[FIELD.HASH],
            }
            return MODIFY.create_event(row['rel_id'], data, _RELATE_VERSION)
        data = {FIELD.LAST_EVENT: row[FIELD.LAST_EVENT]}
        return CONFIRM.create_event(row['rel_id'], data, _RELATE_VERSION)


class Relater:
    model = gob_model
    sources = GOBSources(model)

    space_join = ' \n    '
    comma_join = ',\n    '
    and_join = ' AND\n    '
    or_join = ' OR\n    '

    json_join_alias = 'json_arr_elm'
    src_entities_alias = 'src_entities'
    dst_entities_alias = 'dst_entities'

    # The names of the fields to be returned.
    # Optionally extendes with src_volgnummer and/or dst_volgnummer if applicable.
    select_aliases = [
        FIELD.VERSION,
        FIELD.APPLICATION,
        FIELD.SOURCE_ID,
        FIELD.SOURCE,
        FIELD.EXPIRATION_DATE,
        FIELD.ID,
        FIELD.TID,
        'id',
        'derivation',
        'src_source',
        'src_id',
        'src_last_event',
        'dst_source',
        'dst_id',
        FIELD.SOURCE_VALUE,
        FIELD.LAST_SRC_EVENT,
        FIELD.LAST_DST_EVENT,
        FIELD.START_VALIDITY,
        FIELD.END_VALIDITY,
        'src_deleted',
        'row_number',
    ]

    select_relation_aliases = [
        FIELD.LAST_EVENT,
        'rel_deleted',
        'rel_id',
        'rel_dst_id',
        'rel_dst_volgnummer',
        f'rel_{FIELD.EXPIRATION_DATE}',
        f'rel_{FIELD.START_VALIDITY}',
        f'rel_{FIELD.END_VALIDITY}',
        f'rel_{FIELD.HASH}',
    ]

    def __init__(self, src_catalog_name, src_collection_name, src_field_name):
        self.src_catalog_name = src_catalog_name
        self.src_collection_name = src_collection_name
        self.src_field_name = src_field_name

        try:
            self.src_collection = self.model[src_catalog_name]['collections'][src_collection_name]
        except KeyError:
            self.src_collection = None
        self.src_field = self.src_collection['all_fields'].get(src_field_name)
        self.src_table_name = self.model.get_table_name(src_catalog_name, src_collection_name)

        # Get the destination catalog and collection names
        self.dst_catalog_name, self.dst_collection_name = self.src_field['ref'].split(':')
        try:
            self.dst_collection = self.model[self.dst_catalog_name]['collections'][self.dst_collection_name]
        except KeyError:
            self.dst_collection = None
        self.dst_table_name = self.model.get_table_name(self.dst_catalog_name, self.dst_collection_name)

        # Check if source or destination has states (volgnummer, begin_geldigheid, eind_geldigheid)
        self.src_has_states = self.model.has_states(self.src_catalog_name, self.src_collection_name)
        self.dst_has_states = self.model.has_states(self.dst_catalog_name, self.dst_collection_name)

        # Copy relation specs and set default for multiple_allowed. Filter out specs that are not present in src.
        relation_specs = [{
            **spec,
            'multiple_allowed': spec.get('multiple_allowed', False)
        } for spec in self.sources.get_field_relations(src_catalog_name, src_collection_name, src_field_name)]

        if not relation_specs:
            raise RelateException("Missing relation specification for " +
                                  f"{src_catalog_name} {src_collection_name} {src_field_name} " +
                                  "(sources.get_field_relations)")
        src_applications = self._get_applications_in_src()

        # Only include specs that are present in the src table. If no specs are left this implies that the src table is
        # empty.
        self.relation_specs = [spec for spec in relation_specs if spec['source'] in src_applications]

        self.is_many = self.src_field['type'] == "GOB.ManyReference"
        self.relation_table = "rel_" + get_relation_name(self.model, src_catalog_name, src_collection_name,
                                                         src_field_name)

        # begin_geldigheid tmp table names
        datestr = datetime.now().strftime('%Y%m%d')
        src_intv_tmp_table_name = f"tmp_{self.src_catalog_name}_{self.src_collection['abbreviation']}_intv_{datestr}" \
                                  f"_{random_string(6)}".lower()
        dst_intv_tmp_table_name = f"tmp_{self.dst_catalog_name}_{self.dst_collection['abbreviation']}_intv_{datestr}" \
                                  f"_{random_string(6)}".lower()

        self.src_intv_tmp_table = StartValiditiesTable(self.src_table_name, src_intv_tmp_table_name)
        self.dst_intv_tmp_table = StartValiditiesTable(self.dst_table_name, dst_intv_tmp_table_name)

        self.result_table_name = f"tmp_{self.src_catalog_name}_{self.src_collection['abbreviation']}_" \
                                 f"{src_field_name}_result"

    def __enter__(self):
        self._create_tmp_intv_tables()
        self._create_tmp_result_table()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if DEBUG:
            logger.info("DEBUG ON: Not removing temporary tables")
        else:
            self.src_intv_tmp_table.drop()
            self.dst_intv_tmp_table.drop()
            self._drop_tmp_result_table()

    def _get_applications_in_src(self):
        query = f"SELECT DISTINCT {FIELD.APPLICATION} FROM {self.src_table_name}"

        result = _execute(query)
        return [row[0] for row in result]

    def _validity_select_expressions_src(self):
        if self.src_has_states and self.dst_has_states:
            start = f"GREATEST(src_bg.{FIELD.START_VALIDITY}, dst_bg.{FIELD.START_VALIDITY}, " \
                    f"{self._provided_start_validity()})"
            end = f"LEAST(src.{FIELD.END_VALIDITY}, dst.{FIELD.END_VALIDITY})"
        elif self.src_has_states:
            start = f"GREATEST(src_bg.{FIELD.START_VALIDITY}, {self._provided_start_validity()})"
            end = f"src.{FIELD.END_VALIDITY}"
        elif self.dst_has_states:
            start = f"GREATEST(dst_bg.{FIELD.START_VALIDITY}, {self._provided_start_validity()})"
            end = f"dst.{FIELD.END_VALIDITY}"
        else:
            start = self._provided_start_validity()
            end = "NULL"

        return start, end

    def _select_aliases(self, is_conflicts_query: bool = False):
        return self.select_aliases + (self.select_relation_aliases if not is_conflicts_query else []) \
               + (['src_volgnummer'] if self.src_has_states else []) \
               + (['dst_volgnummer'] if self.dst_has_states else [])

    def _build_select_expressions(self, mapping: dict, is_conflicts_query: bool = False):
        aliases = self._select_aliases(is_conflicts_query)

        assert all(alias in mapping.keys() for alias in aliases), \
            'Missing key(s): ' + str([alias for alias in aliases if alias not in mapping.keys()])

        return [f'{mapping[alias]} AS {alias}' for alias in aliases]

    def _select_expressions_src(self, max_src_event: int, max_dst_event: int, is_conflicts_query: bool = False):
        """Returns the select expressions for the src query

        :return:
        """
        start_validity, end_validity = self._validity_select_expressions_src()

        mapping = {
            FIELD.VERSION: f"'{_RELATE_VERSION}'",
            FIELD.APPLICATION: f"src.{FIELD.APPLICATION}",
            FIELD.SOURCE_ID: self._get_id(),
            FIELD.SOURCE: f"'{GOB}'",
            FIELD.EXPIRATION_DATE: f"LEAST(src.{FIELD.EXPIRATION_DATE}, dst.{FIELD.EXPIRATION_DATE})",
            FIELD.ID: self._get_id(),
            FIELD.TID: self._get_id(),
            "id": self._get_id(),
            "derivation": self._get_derivation(),
            "src_source": f"src.{FIELD.SOURCE}",
            "src_id": f"src.{FIELD.ID}",
            "src_volgnummer": f"src.{FIELD.SEQNR}",
            "src_last_event": f"src.{FIELD.LAST_EVENT}",
            "dst_source": f"dst.{FIELD.SOURCE}",
            "dst_id": f"dst.{FIELD.ID}",
            "dst_volgnummer": f"dst.{FIELD.SEQNR}",
            FIELD.SOURCE_VALUE: self._source_value_ref(),
            FIELD.LAST_SRC_EVENT: f"'{max_src_event}'",
            FIELD.LAST_DST_EVENT: f"'{max_dst_event}'",
            FIELD.LAST_EVENT: f"rel.{FIELD.LAST_EVENT}",
            "src_deleted": f"src.{FIELD.DATE_DELETED}",
            "rel_deleted": f"rel.{FIELD.DATE_DELETED}",
            "rel_id": "rel.id",
            "rel_dst_id": "rel.dst_id",
            "rel_dst_volgnummer": "rel.dst_volgnummer",
            f"rel_{FIELD.EXPIRATION_DATE}": f"rel.{FIELD.EXPIRATION_DATE}",
            f"rel_{FIELD.START_VALIDITY}": f"rel.{FIELD.START_VALIDITY}",
            f"rel_{FIELD.END_VALIDITY}": f"rel.{FIELD.END_VALIDITY}",
            f"rel_{FIELD.HASH}": f"rel.{FIELD.HASH}",
            FIELD.START_VALIDITY: f"CASE WHEN dst.{FIELD.ID} IS NULL THEN NULL::timestamp without time zone "
                                  f"ELSE {start_validity} END",
            FIELD.END_VALIDITY: f"CASE WHEN dst.{FIELD.ID} IS NULL THEN NULL::timestamp without time zone "
                                f"ELSE {end_validity} END",
            "row_number": "row_number",
        }

        select_expressions = self._build_select_expressions(mapping, is_conflicts_query)

        return select_expressions

    def _select_expressions_rel_delete(self):
        mapping = {alias: 'NULL' for alias in self._select_aliases()}
        mapping.update({
            FIELD.EXPIRATION_DATE: "NULL::timestamp without time zone",
            FIELD.LAST_EVENT: f"rel.{FIELD.LAST_EVENT}",
            "src_last_event": "NULL::integer",
            FIELD.LAST_SRC_EVENT: "NULL::integer",
            FIELD.LAST_DST_EVENT: "NULL::integer",
            "src_volgnummer": "NULL::integer",
            "dst_volgnummer": "NULL::integer",
            FIELD.START_VALIDITY: "NULL::timestamp without time zone",
            FIELD.END_VALIDITY: "NULL::timestamp without time zone",
            "src_deleted": "NULL::timestamp without time zone",
            "rel_deleted": "NULL::timestamp without time zone",
            "row_number": "NULL::integer",
            'rel_id': 'rel.id',
            'rel_dst_id': 'rel.dst_id',
            'rel_dst_volgnummer': 'rel.dst_volgnummer',
            f"rel_{FIELD.EXPIRATION_DATE}": f"rel.{FIELD.EXPIRATION_DATE}",
            f"rel_{FIELD.START_VALIDITY}": f"rel.{FIELD.START_VALIDITY}",
            f"rel_{FIELD.END_VALIDITY}": f"rel.{FIELD.END_VALIDITY}",
            f"rel_{FIELD.HASH}": f"rel.{FIELD.HASH}",
        })

        return self._build_select_expressions(mapping)

    def _get_id_for_spec(self, spec: dict, src_value_ref=None):
        """Creates ID for source specification.

        Default ID is: [src_id](.[src_volgnummer]).[src_source].[bronwaarde]

        If multiple_allowed is set to true for given source specification, the destination is added, so that this
        row is uniquely identifiable in the relation table.

        With multiple_allowed: [src_id](.[src_volgnummer]).[src_source].[bronwaarde].[dst_id](.[dst_volgnummer]

        :param spec:
        :return:
        """
        src_value_ref = src_value_ref or self._source_value_ref()
        id_fields = [f"src.{FIELD.ID}"]

        if self.src_has_states:
            id_fields += [f"src.{FIELD.SEQNR}"]

        id_fields += [
            f"src.{FIELD.SOURCE}",
            f"({src_value_ref})"
        ]

        if spec['multiple_allowed']:
            id_fields += [f"dst.{FIELD.ID}"]

            if self.dst_has_states:
                id_fields += [f"dst.{FIELD.SEQNR}"]

        return " || '.' || ".join(id_fields)

    def _get_id_for_dst(self):
        return self._get_id('src.bronwaarde')

    def _get_id(self, src_value_ref=None):

        if not self.relation_specs:
            return 'NULL'
        if len(self.relation_specs) == 1:
            # Only one spec, no CASE expression necessary
            return self._get_id_for_spec(self.relation_specs[0], src_value_ref)

        # Switch per source. Different sources have different multiple_allowed values
        whens = [f"WHEN src.{FIELD.APPLICATION} = '{spec['source']}' THEN {self._get_id_for_spec(spec, src_value_ref)}"
                 for spec in self.relation_specs]

        newline = '\n'
        return f"CASE {newline.join(whens)}\nEND"

    def _get_derivation(self):
        """Returns CASE statement for derivation in select expressions.

        :return:
        """
        if not self.relation_specs:
            return 'NULL'
        if len(self.relation_specs) == 1:
            # Only one spec, no CASE expression necessary
            return f"'{self.relation_specs[0]['destination_attribute']}'"

        whens = "\n        ".join(
            [f"WHEN '{spec['source']}' THEN '{spec['destination_attribute']}'" for spec in self.relation_specs]
        )
        return f'CASE src.{FIELD.APPLICATION}\n        {whens}\n    END'

    def _source_value_ref(self):
        """Returns the reference to the source value in the src object. For a many relation this reference points to
        the unpacked JSONB object.

        :return:
        """

        if self.is_many:
            return f"{self.json_join_alias}.item->>'{FIELD.SOURCE_VALUE}'"
        return f"src.{self.src_field_name}->>'{FIELD.SOURCE_VALUE}'"

    def _provided_start_validity(self):
        """Returns the start validity as provided in the src object, if present. Defaults to NULL if not present.

        :return:
        """

        if self.is_many:
            return f"({self.json_join_alias}.item->>'{FIELD.START_VALIDITY}')::timestamp without time zone"
        return f"(src.{self.src_field_name}->>'{FIELD.START_VALIDITY}')::timestamp without time zone"

    def _geo_resolve(self, spec):
        src_geo = f"src.{spec['source_attribute']}"
        dst_geo = f"dst.{spec['destination_attribute']}"

        resolvers = {
            LIES_IN: f"ST_IsValid({dst_geo}) "
                     f"AND ST_Contains({dst_geo}::geometry, ST_PointOnSurface({src_geo}::geometry))"
        }
        return resolvers.get(spec["method"])

    def _json_obj_ref(self):
        return f'{self.json_join_alias}.item' if self.is_many else f'src.{self.src_field_name}'

    def _relate_match(self, spec, source_value_ref=None):
        if spec.get('source_attribute'):
            # source_attribute is defined, use it instead of bronwaarde
            source_value_ref = f"src.{spec['source_attribute']}"
        elif not source_value_ref:
            source_value_ref = f"{self._json_obj_ref()}->>'bronwaarde'"

        if spec['method'] == EQUALS:
            return f"dst.{spec['destination_attribute']} = {source_value_ref}"
        return self._geo_resolve(spec)

    def _src_dst_match(self, source_value_ref=None):
        """Returns the match clause to match src and dst, to be used in an ON clause (or WHERE, for that matter)

        :param src_ref:
        :return:
        """

        if len(self.relation_specs) == 1:
            clause = [self._relate_match(self.relation_specs[0], source_value_ref)]
        else:
            clause = [f"(src.{FIELD.APPLICATION} = '{spec['source']}' AND " +
                      f"{self._relate_match(spec, source_value_ref)})" for spec in self.relation_specs]

        # If more matches have been defined that catch any of the matches
        if len(clause) > 1:
            clause = [f"({self.or_join.join(clause)})"]
        # If both collections have states then join with corresponding geldigheid intervals
        if self.src_has_states and self.dst_has_states:
            # One of two cases may apply

            # The 'normal' case, where the START_VALIDITY and END_VALIDITY of the src object are NOT equal. This means
            # that this particular has a validity length > 0. END_VALIDITY may be NULL, in which case the validity
            # length is infinite.
            # We use < and >= to match states, so that corresponding states on the src and dst objects, where
            # START_VALIDITY's and END_VALIDITY's of src and dst line up, are matched correctly.
            normal_case = self.and_join.join([
                f"(dst.{FIELD.START_VALIDITY} < src.{FIELD.END_VALIDITY} OR src.{FIELD.END_VALIDITY} IS NULL)",
                f"(dst.{FIELD.END_VALIDITY} >= src.{FIELD.END_VALIDITY} OR dst.{FIELD.END_VALIDITY} IS NULL)",
                f"(src.{FIELD.END_VALIDITY} IS NULL OR src.{FIELD.START_VALIDITY} <> src.{FIELD.END_VALIDITY})",
            ])

            # The 'collapsed state' case. Here the START_VALIDITY and the END_VALIDITY of the src object are equal,
            # meaning that the validity length of this object = 0.
            # The difference with the 'normal' case is that we use both <= and >= to match states, so that a length 0
            # state will find a match if anything matches on that particular point in time.
            collapsed_state_case = self.and_join.join([
                f"(dst.{FIELD.START_VALIDITY} <= src.{FIELD.END_VALIDITY})",
                f"(dst.{FIELD.END_VALIDITY} >= src.{FIELD.END_VALIDITY} OR dst.{FIELD.END_VALIDITY} IS NULL)",
                f"(src.{FIELD.START_VALIDITY} = src.{FIELD.END_VALIDITY})",
            ])

            clause.append(f'({self.or_join.join([f"({normal_case})", f"({collapsed_state_case})"])})')

        elif self.dst_has_states:
            # If only destination has states, get the destination that is valid until forever
            clause.extend([
                f"(dst.{FIELD.END_VALIDITY} IS NULL OR dst.{FIELD.END_VALIDITY} > NOW())"
            ])

        return clause

    def _dst_table_outer_join_on(self):
        join_on = [f"dst.{FIELD.ID} = src_dst.dst_id"]

        if self.dst_has_states:
            join_on.append(f"dst.{FIELD.SEQNR} = src_dst.dst_volgnummer")

        return join_on

    def _src_dst_join_on(self):

        join_on = [f"src_dst.src_id = src.{FIELD.ID}"]
        if self.src_has_states:
            join_on.append(f"src_dst.src_volgnummer = src.{FIELD.SEQNR}")

        join_on += [
            f"src_dst.{FIELD.SOURCE} = src.{FIELD.SOURCE}",
            f"src_dst.bronwaarde = {self._json_obj_ref()}->>'bronwaarde'",
        ]

        join_on.append("src_dst.dst_id IS NOT NULL")

        return join_on

    def _src_dst_select_expressions(self):
        expressions = [f"src.{FIELD.ID} AS src_id",
                       f"dst.{FIELD.ID} AS dst_id",
                       f"{self._json_obj_ref()}->>'bronwaarde' AS bronwaarde",
                       f"src.{FIELD.SOURCE}",
                       f"{self._row_number_partition()} AS row_number"]

        if self.src_has_states:
            expressions.append(f"src.{FIELD.SEQNR} AS src_volgnummer")

        if self.dst_has_states:
            expressions.append(f"max(dst.{FIELD.SEQNR}) AS dst_volgnummer")

        return expressions

    def _src_dst_group_by(self):
        group_by = [f"src.{FIELD.ID}", f"dst.{FIELD.ID}", "bronwaarde", f"src.{FIELD.SOURCE}"]

        if self.src_has_states:
            group_by.append(f"src.{FIELD.SEQNR}")
        return group_by

    def _have_geo_specs(self):
        """Returns True if this relation includes a geo match.

        :return:
        """
        return any(spec['method'] != EQUALS for spec in self.relation_specs)

    def _valid_geo_src_check(self):
        """Returns the proper ST_IsValid checks for the src geo fields

        :return:
        """

        return self._switch_for_specs(
            'source_attribute',
            lambda spec: f"ST_IsValid({spec['source_attribute']})"
        )

    def _join_array_elements(self):
        return f"JOIN jsonb_array_elements(src.{self.src_field_name}) {self.json_join_alias}(item) " \
               f"ON {self.json_join_alias}->>'{FIELD.SOURCE_VALUE}' IS NOT NULL"

    def _src_dst_join(self, dst_entities: str):
        """Generated twice, once to generate relations from a subset of the src relation to all the dst relations
        (source_side='src'), and once to generate the remaining relations from a subset of the dst relations to all
        the src relations (source_side='dst') that were not included in the first step

        :param source_side:
        :return:
        """
        join_on = self._src_dst_match()
        join_on.append(f"dst.{FIELD.DATE_DELETED} IS NULL")

        if self._have_geo_specs():
            """If this relation contains geometry matching, we should include a validity check in the query. This means
            that we add an extra join on the source table in which we check the validity of the geo fields in the src
            table.
            Also, instead of directly joining the dst table, we join with a subset of the dst table containing only
            rows that have valid geometries
            """
            validgeo_src = \
                f"JOIN (SELECT * FROM {self.src_table_name} WHERE ({self._valid_geo_src_check()})) valid_src " \
                f"ON src.{FIELD.GOBID} = valid_src.{FIELD.GOBID}"
        else:
            # Nothing special here
            validgeo_src = ""

        return f"""
INNER JOIN (
    SELECT
        {self.comma_join.join(self._src_dst_select_expressions())}
    FROM (
        SELECT * FROM {self.src_entities_alias} WHERE {FIELD.DATE_DELETED} IS NULL
    ) src
    {validgeo_src}
    {self._join_array_elements() if self.is_many else ""}
    LEFT JOIN ({dst_entities}) dst ON {self.and_join.join(join_on)}
    GROUP BY {self.comma_join.join(self._src_dst_group_by())}
) src_dst ON {self.and_join.join(self._src_dst_join_on())}
"""

    def _row_number_partition(self, source_value_ref=None):
        """Returns the row_number() window function to avoid duplicate (src_id, [src_volgummer,] bronwaarde) rows in
        the relation table. Partitions by src_id, src_volgnummer and bronwaarde, so that we can select only the row
        where the row number = 1. Rows within a partition are ordered by bronwaarde.

        :return:
        """
        source_value_ref = source_value_ref if source_value_ref is not None else self._source_value_ref()

        partition_by = [
            f"src.{FIELD.ID}",
            f"src.{FIELD.SEQNR}",
            source_value_ref
        ] if self.src_has_states else [
            f"src.{FIELD.ID}",
            source_value_ref
        ]

        return f"row_number() OVER (PARTITION BY {','.join(partition_by)} ORDER BY {source_value_ref})"

    def _select_rest_src(self):
        not_in_fields = [FIELD.ID, FIELD.SEQNR] if self.src_has_states else [FIELD.ID]
        source_value_not_null = f" AND {self._source_value_ref()} IS NOT NULL" if not self.is_many else ""

        return f"""
    SELECT
        src.*,
        {self._source_value_ref()} {FIELD.SOURCE_VALUE},
        {self._provided_start_validity()} provided_{FIELD.START_VALIDITY}
    FROM {self.src_table_name} src
    {self._join_array_elements() if self.is_many else ""}
    WHERE src.{FIELD.DATE_DELETED} IS NULL AND ({','.join(not_in_fields)}) NOT IN (
        SELECT {','.join(not_in_fields)} FROM {self.src_entities_alias}
    ){source_value_not_null}
"""

    def _create_tmp_intv_tables(self):
        """Creates tmp tables from recursive queries to determine the begin_geldigheid for each volgnummer."""
        if self.src_has_states:
            logger.info(f"Creating temporary table {self.src_intv_tmp_table.name}")
            self.src_intv_tmp_table.create()

        if self.dst_has_states:
            if self.src_table_name == self.dst_table_name:
                # Use same table as src as they are the same
                self.dst_intv_tmp_table = self.src_intv_tmp_table
            else:
                logger.info(f"Creating temporary table {self.dst_intv_tmp_table.name}")
                self.dst_intv_tmp_table.create()

    def _drop_tmp_result_table(self):
        _execute(f"DROP TABLE IF EXISTS {self.result_table_name}")

    def _create_tmp_result_table(self):
        varchar = 'varchar'
        serial = 'serial'
        timestamp = 'timestamp'
        integer = 'integer'

        types = {
            'rowid': serial,
            FIELD.VERSION: varchar,
            FIELD.APPLICATION: varchar,
            FIELD.SOURCE_ID: varchar,
            FIELD.SOURCE: varchar,
            FIELD.EXPIRATION_DATE: timestamp,
            FIELD.ID: varchar,
            FIELD.TID: varchar,
            'id': varchar,
            'derivation': varchar,
            'src_source': varchar,
            'src_id': varchar,
            'src_last_event': integer,
            'dst_source': varchar,
            'dst_id': varchar,
            FIELD.SOURCE_VALUE: varchar,
            FIELD.LAST_SRC_EVENT: integer,
            FIELD.LAST_DST_EVENT: integer,
            FIELD.START_VALIDITY: timestamp,
            FIELD.END_VALIDITY: timestamp,
            'src_deleted': timestamp,
            'row_number': integer,
            FIELD.LAST_EVENT: integer,
            'rel_deleted': timestamp,
            'rel_id': varchar,
            'rel_dst_id': varchar,
            'rel_dst_volgnummer': integer,
            f'rel_{FIELD.EXPIRATION_DATE}': timestamp,
            f'rel_{FIELD.START_VALIDITY}': timestamp,
            f'rel_{FIELD.END_VALIDITY}': timestamp,
            f'rel_{FIELD.HASH}': varchar,
            'src_volgnummer': integer,
            'dst_volgnummer': integer
        }

        columns = ['rowid'] + self._select_aliases()

        column_list = ",\n".join([f"    {column} {types[column]}" for column in columns])
        query = f"CREATE UNLOGGED TABLE IF NOT EXISTS {self.result_table_name} (\n{column_list}\n)"

        logger.info(f"Create temporary results table {self.result_table_name}")
        _execute(query)
        _execute("TRUNCATE " + self.result_table_name)

    def _src_entities_range(self, min_src_event_id: int, max_src_event_id: int = None):
        filters = []

        if not self.is_many:
            filters.append(f"{self._source_value_ref()} IS NOT NULL")

        if min_src_event_id:
            filters.append(f"src.{FIELD.LAST_EVENT} > {min_src_event_id}")

        if max_src_event_id:
            filters.append(f"src.{FIELD.LAST_EVENT} <= {max_src_event_id}")

        filters_str = f'WHERE {" AND ".join(filters)}' if filters else ""

        return f"""
SELECT * FROM {self.src_table_name} src
{filters_str}
"""

    def _dst_entities_range(self, min_dst_event_id: int = None, max_dst_event_id: int = None):
        filters = []

        if min_dst_event_id:
            filters.append(f"dst.{FIELD.LAST_EVENT} > {min_dst_event_id}")

        if max_dst_event_id:
            filters.append(f"dst.{FIELD.LAST_EVENT} <= {max_dst_event_id}")

        filters_str = f'WHERE {" AND ".join(filters)}' if filters else ""

        return f"""
SELECT * FROM {self.dst_table_name} dst
{filters_str}
"""

    def _join_geldigheid(self, table_name: str, alias: str, join_with: str):
        """Returns the join with the begin_geldigheid for the given volgnummer for either 'src' or 'dst' (src_bg or
        dst_bg)
        """
        return f"LEFT JOIN {table_name} {alias} " \
               f"ON {alias}.{FIELD.ID} = {join_with}.{FIELD.ID} " \
               f"AND {alias}.{FIELD.SEQNR} = {join_with}.{FIELD.SEQNR}"

    def _join_dst_geldigheid(self):
        return self._join_geldigheid(self.dst_intv_tmp_table.name, 'dst_bg', 'dst') if self.dst_has_states else ""

    def _join_src_geldigheid(self):
        return self._join_geldigheid(self.src_intv_tmp_table.name, 'src_bg', 'src') if self.src_has_states else ""

    def _join_rel_dst_clause(self):
        dst_join_clause = f"rel.dst_id = dst.{FIELD.ID}"

        if self.dst_has_states:
            dst_join_clause += f" AND rel.dst_volgnummer = dst.{FIELD.SEQNR}"

        return self._switch_for_specs(
            'multiple_allowed',
            lambda spec: dst_join_clause if spec['multiple_allowed'] else 'TRUE'
        )

    def _join_rel(self):
        query = [
            f"LEFT JOIN {self.relation_table} rel",
            f"ON rel.src_id = src.{FIELD.ID}",
            f"AND rel.src_source = src.{FIELD.SOURCE}",
            f"AND {self._source_value_ref()} = rel.{FIELD.SOURCE_VALUE}",
            f"AND {self._join_rel_dst_clause()}"
        ]
        if self.src_has_states:
            query.insert(2, f"AND rel.src_volgnummer = src.{FIELD.SEQNR}")

        return ' '.join(query)

    def _filter_conflicts(self):
        """Returns row_number check per source. Skip sources that have multiple_allowed set to True.
        Only used in the conflicts query.

        :return:
        """

        return self._switch_for_specs(
            'multiple_allowed',
            lambda spec: 'row_number > 1' if not spec['multiple_allowed'] else 'FALSE'
        )

    def _get_where(self, is_conflicts_query: bool = False):
        """Returns WHERE clause for both sides of the UNION.
        Only include not-deleted relations or existing sources.

        Reason is that sometimes we do need deleted relations, in case the src is matched with a previously deleted
        row in the relation table. We will need the last event of that row to re-add that row.

        :return:
        """
        has_multiple_allowed_source = any(spec['multiple_allowed'] for spec in self.relation_specs)
        return "WHERE (" \
               + (f"rel.{FIELD.DATE_DELETED} IS NULL OR " if not is_conflicts_query else "") \
               + f"src.{FIELD.ID} IS NOT NULL)" \
               + f" AND dst.{FIELD.DATE_DELETED} IS NULL" \
               + (f" AND {self._filter_conflicts()}" if is_conflicts_query else "") \
               + (f" AND {self._multiple_allowed_where()}" if has_multiple_allowed_source else "")

    def _multiple_allowed_where(self):
        """If any of the sources for this relation has multiple_allowed set to true, add this extra where-clause

        :return:
        """

        return self._switch_for_specs(
            'multiple_allowed',
            lambda spec: f'dst.{FIELD.ID} IS NOT NULL' if spec['multiple_allowed'] else 'TRUE'
        )

    def _switch_for_specs(self, attribute: str, func: callable):
        """Creates a conditional expression based on the different sources, for example, creates constructs like:

        ... AND ((src._application = 'A' AND row_number = 1) OR (src._application = 'B' AND row_number = 1) OR
            (src._application = 'C' AND some_other_condition))

        where the expressions (AND row_number = 1 in this case) are conditional based on the value of src._application.

        Tries to simplify the condition. If the expression that comes after the _application comparison is the same
        for all values for _application, the comparison with _application is omitted.

        :param attribute:
        :param func:
        :return:
        """

        if self._can_simplify_for(attribute):
            return func(self.relation_specs[0])
        ors = []

        for spec in self.relation_specs:
            condition = func(spec)

            if condition == 'FALSE':
                continue

            # simplify TRUE
            condition = '' if condition == 'TRUE' else f" AND {condition}"
            ors.append(f"src.{FIELD.APPLICATION} = '{spec['source']}'{condition}")

        if not ors:
            # No possible matches. Always FALSE. Important to add this if nothing matches above, otherwise this
            # would result in a (wrongly) implied TRUE (everything matches).
            return 'FALSE'

        return f"(({') OR ('.join(ors)}))"

    def _can_simplify_for(self, attribute: str):
        """Expressions like ((_source = 'A' AND somecondition) OR (_source = 'B' and somecondition) can often be
        simplified, because the condition depends on a value in the spec in relation_specs. If all specs in
        relation_specs have the same value for the referred attribute, we can simplify this expression, with positive
        impact on database performance.

        This method returns a boolean value if such constructs based on the value of attribute can be simplified. (That
        is, if for all specs in relation_specs the value for attribute is the same).

        :param attribute:
        :return:
        """
        return len(set([spec[attribute] for spec in self.relation_specs])) == 1

    def _create_delete_events_query(self, min_src_event: int, max_src_event: int, min_dst_event: int,
                                    max_dst_event: int):
        """Returns the query that adds the relations to delete to the result table.

        The relations to delete are:
        All rows currently in the relation table,
        - that are either related on the src side, or on the dst side (the first (inner) JOIN),
        - which are not already deleted (so _date_deleted IS NULL),
        - and not already present in the result table (the LEFT JOIN, combined with res.rel_id IS NULL

        Both src and dst are checked separately on both sides of the UNION

        :param min_src_event:
        :param max_src_event:
        :param min_dst_event:
        :param max_dst_event:
        :return:
        """
        src_join = f"rel.src_id = src.{FIELD.ID}" + (f" AND rel.src_volgnummer = src.{FIELD.SEQNR}"
                                                     if self.src_has_states else "")
        dst_join = f"rel.dst_id = dst.{FIELD.ID}" + (f" AND rel.dst_volgnummer = dst.{FIELD.SEQNR}"
                                                     if self.dst_has_states else "")
        return f"""
SELECT {self.comma_join.join(self._select_expressions_rel_delete())}
FROM {self.relation_table} rel
WHERE rel.id IN (
    SELECT rel.id FROM {self.relation_table} rel
    JOIN {self.src_table_name} src ON {src_join}
      AND src.{FIELD.LAST_EVENT} > {min_src_event} AND src.{FIELD.LAST_EVENT} <= {max_src_event}
    LEFT JOIN {self.result_table_name} res ON res.rel_id = rel.id
    WHERE rel.{FIELD.DATE_DELETED} IS NULL AND res.rel_id IS NULL
    UNION
    SELECT rel.id FROM {self.relation_table} rel
    JOIN {self.dst_table_name} dst ON {dst_join}
      AND dst.{FIELD.LAST_EVENT} > {min_dst_event} AND dst.{FIELD.LAST_EVENT} <= {max_dst_event}
    LEFT JOIN {self.result_table_name} res ON res.rel_id = rel.id
    WHERE rel.{FIELD.DATE_DELETED} IS NULL AND res.rel_id IS NULL
)
"""

    def get_query(self, src_entities: str, dst_entities: str, max_src_event: int, max_dst_event: int,
                  is_conflicts_query=False):
        return f"""
WITH
{self.src_entities_alias} AS ({src_entities})
SELECT
    {self.comma_join.join(self._select_expressions_src(max_src_event, max_dst_event, is_conflicts_query))}
FROM {self.src_entities_alias} src
{self._join_array_elements() if self.is_many else ""}
{self._src_dst_join(dst_entities)}
LEFT JOIN {self.dst_table_name} dst ON {self.and_join.join(self._dst_table_outer_join_on())}
{self._join_rel() if not is_conflicts_query else ""}
{self._join_src_geldigheid()}
{self._join_dst_geldigheid()}
{self._get_where(is_conflicts_query)}
"""

    def get_full_query(self, is_conflicts_query=False):
        max_src_event = self._get_max_src_event()
        src_entities = self._src_entities_range(0, max_src_event)
        dst_entities = self._dst_entities_range()
        return self.get_query(src_entities, dst_entities, max_src_event, self._get_max_dst_event(), is_conflicts_query)

    def _format_relation(self, relation: dict):
        # Remove row_number from the relation
        relation.pop('row_number', None)

        for ts_field in FIELD.START_VALIDITY, FIELD.END_VALIDITY, FIELD.EXPIRATION_DATE:
            value = relation.get(ts_field)

            # isinstance check is not sufficient here: isinstance(<datetime value>, date) is True
            if type(value) is date:
                relation[ts_field] = datetime.combine(value, datetime.min.time())

        return relation

    def _is_initial_load(self) -> bool:
        query = f"SELECT {FIELD.ID} FROM {self.relation_table} LIMIT 1"
        result = _execute(query)

        try:
            next(result)
        except StopIteration:
            logger.info("Relation table is empty. Have initial load.")
            return True
        return False

    def _check_preconditions(self):
        """Checks if all applications in the src table are defined in GOBSources.

        Relate can't reliably happen if that is not the case.
        :return:
        """

        applications = [spec['source'] for spec in self.relation_specs]
        src_table_applications = self._get_applications_in_src()

        difference = set(src_table_applications) - set(applications)

        if difference:
            raise GOBException(f"Can't relate {self.src_catalog_name} {self.src_collection_name} "
                               f"{self.src_field_name} because the src table contains values for "
                               f"{FIELD.APPLICATION} that are not defined in GOBSources: {','.join(difference)}")

    def _get_max_src_event(self) -> int:
        query = f"SELECT MAX({FIELD.LAST_EVENT}) FROM {self.src_table_name}"
        return next(_execute(query))[0] or 0

    def _get_max_dst_event(self) -> int:
        query = f"SELECT MAX({FIELD.LAST_EVENT}) FROM {self.dst_table_name}"
        return next(_execute(query))[0] or 0

    def _get_min_src_event(self) -> int:
        query = f"SELECT MAX({FIELD.LAST_SRC_EVENT}) FROM {self.relation_table}"
        return next(_execute(query))[0] or 0

    def _get_min_dst_event(self) -> int:
        query = f"SELECT MAX({FIELD.LAST_DST_EVENT}) FROM {self.relation_table}"
        return next(_execute(query))[0] or 0

    def _get_next_max_src_event(self, start_eventid: int, max_rows: int, max_eventid: int) -> int:
        """Gets next max src event, counting max :max_rows: from :start_eventid:, respecting :max_eventid:
        Unpacks jsonb column for ManyReferences, to account for possible large relations (otherwise 30k src rows may
        grow to 350k src rows when unpacked).

        :param start_eventid:
        :param max_rows:
        :param max_eventid:
        :return:
        """
        query = f"SELECT src.{FIELD.LAST_EVENT} " \
                f"FROM {self.src_table_name} src " \
                f"{(self._join_array_elements() if self.is_many else '')} " \
                f"WHERE src.{FIELD.LAST_EVENT} > {start_eventid} AND src.{FIELD.LAST_EVENT} <= {max_eventid} " \
                f"ORDER BY src.{FIELD.LAST_EVENT} " \
                f"OFFSET {max_rows} - 1 " \
                f"LIMIT 1"

        try:
            return next(_execute(query))[0]
        except StopIteration:
            return max_eventid

    def _get_next_max_dst_event(self, start_eventid: int, max_rows: int, max_eventid: int):
        query = f"SELECT {FIELD.LAST_EVENT} " \
                f"FROM {self.dst_table_name} " \
                f"WHERE {FIELD.LAST_EVENT} > {start_eventid} AND {FIELD.LAST_EVENT} <= {max_eventid} " \
                f"ORDER BY {FIELD.LAST_EVENT} " \
                f"OFFSET {max_rows} - 1 " \
                f"LIMIT 1"

        try:
            return next(_execute(query))[0]
        except StopIteration:
            return max_eventid

    def _get_chunks(self, start_src_event: int, max_src_event: int, start_dst_event: int, max_dst_event: int,
                    only_src_side: bool = False):
        """Generates the queries to select the src_entities and dst_entities to consider for each iteration of the
        relate process.

        Generation of queries happens in two steps:
        - First we loop through chunks of all changed entities on the src side and relate that to all dst entities up
          until max_dst_event.
        - Second, we loop through all changed dst entities and relate that to all src entities not considered in the
          previous step.

        In numbers, what is considered in both steps:
        - First: all src events > start_src_event and <= max_src_event, and all dst > 0 and <= max_dst_event
        - Second: all src events <= start_src_event and all dst events > start_dst_event and <= max_dst_event

        The value of _MAX_ROWS_PER_SIDE is respected in both steps (on the src side for the first step, on the dst
        side for the second step).

        :param start_src_event:
        :param max_src_event:
        :param start_dst_event:
        :param max_dst_event:
        :return:
        """

        current_min_src_event = start_src_event
        while current_min_src_event < max_src_event:
            current_max_src_event = self._get_next_max_src_event(current_min_src_event, _MAX_ROWS_PER_SIDE,
                                                                 max_src_event)

            yield (self._src_entities_range(current_min_src_event, current_max_src_event),
                   self._dst_entities_range())
            current_min_src_event = current_max_src_event

        if not only_src_side:
            current_min_dst_event = start_dst_event
            while current_min_dst_event < max_dst_event:
                current_max_dst_event = self._get_next_max_dst_event(current_min_dst_event, _MAX_ROWS_PER_SIDE,
                                                                     max_dst_event)

                yield (self._src_entities_range(0, start_src_event),
                       self._dst_entities_range(current_min_dst_event, current_max_dst_event))
                current_min_dst_event = current_max_dst_event

    def _get_updates_chunked(self, start_src_event: int, max_src_event: int, start_dst_event: int,
                             max_dst_event: int, only_src_side: bool = False, is_conflicts_query: bool = False):
        """Inserts updates in the results table using chunks -> Only relating small portions of the total set each
        interation.

        :param start_src_event:
        :param max_src_event:
        :param start_dst_event:
        :param max_dst_event:
        :return:
        """

        for src_entities_query, dst_entities_query in self._get_chunks(start_src_event, max_src_event, start_dst_event,
                                                                       max_dst_event, only_src_side=only_src_side):
            query = self.get_query(src_entities_query, dst_entities_query, max_src_event, max_dst_event,
                                   is_conflicts_query=is_conflicts_query)
            self._query_into_results_table(query, is_conflicts_query)

        self._remove_duplicate_rows()

    def _remove_duplicate_rows(self):
        """Removes duplicate rows from results table that may have been inserted in the table.
        Duplicates will be found and returned as errors in the check relate process. Here we just ignore duplicates.

        Keeps the highest volgnummer of the (lexicographically) first dst id. Keeping the highest volgnummer is
        important, because relating in batches can cause multiple volgnummers to be matched. We always want to keep the
        highest, because that what the result would be without relating in batches.
        :return:
        """

        order_by = "dst_id, dst_volgnummer DESC" if self.dst_has_states else "dst_id"
        query = f"DELETE FROM {self.result_table_name} " \
                f"WHERE rowid IN (" \
                f"  SELECT rowid " \
                f"  FROM (" \
                f"    SELECT rowid, row_number() OVER (PARTITION BY id ORDER BY {order_by}) rn " \
                f"    FROM {self.result_table_name}" \
                f"  ) q WHERE rn > 1" \
                f")"

        return _execute(query)

    def _query_into_results_table(self, query: str, is_conflicts_query: bool = False):
        result_table_columns = ', '.join(self._select_aliases(is_conflicts_query))
        return _execute(f"INSERT INTO {self.result_table_name} ({result_table_columns}) ({query})")

    def _get_updates(self, initial_load: bool = False):
        """Relates in chunks. Chunks are determined by the _get_chunks method.

        Only checking relations to/from src/dst objects that may have changed since the last relate.
        :return:
        """
        start_src_event, max_src_event, start_dst_event, max_dst_event = self._get_changed_ranges()

        if initial_load:
            self._get_updates_full(max_src_event, max_dst_event)
        else:
            self._get_updates_chunked(start_src_event, max_src_event, start_dst_event, max_dst_event)

        # Add delete event rows
        query = self._create_delete_events_query(start_src_event, max_src_event, start_dst_event, max_dst_event)
        self._query_into_results_table(query)

        for row in self._read_results():
            yield dict(row)

    def _read_results(self):
        yield from _execute(f"SELECT * FROM {self.result_table_name}", stream=True, max_row_buffer=25000)

    def _get_changed_ranges(self):
        """Returns a tuple (start_src_event, max_src_event, start_dst_event, max_dst_event) of objects to consider
        when relating only the updates.

        The start of the intervals (start_src_event, start_dst_event) are open. End of the intervals (max_src_event,
        max_dst_event) are closed.

        :return:
        """
        start_src_event = self._get_min_src_event()
        start_dst_event = self._get_min_dst_event()
        max_src_event = self._get_max_src_event()
        max_dst_event = self._get_max_dst_event()

        return start_src_event, max_src_event, start_dst_event, max_dst_event

    def _get_updates_full(self, max_src_event: int, max_dst_event: int, is_conflicts_query=False):
        self._get_updates_chunked(0, max_src_event, 0, max_dst_event, only_src_side=True,
                                  is_conflicts_query=is_conflicts_query)

    def _get_count_for(self, frm: str):
        result = _execute(f"SELECT COUNT(*) FROM {frm} alias")
        return next(result)[0]

    def _force_full_relate(self) -> bool:
        """Returns True if should force full relate. Full relate is forced when there are many updated src/dst objects
        to consider.

        :return:
        """
        start_src_event, max_src_event, start_dst_event, max_dst_event = self._get_changed_ranges()

        total_src_objects = self._get_count_for(self.src_table_name)
        total_dst_objects = self._get_count_for(self.dst_table_name)

        changed_src_objects = self._get_count_for(f'({self._src_entities_range(start_src_event, max_src_event)})')
        changed_dst_objects = self._get_count_for(f'({self._dst_entities_range(start_dst_event, max_dst_event)})')

        src_ratio = changed_src_objects / total_src_objects if total_src_objects > 0 else 1
        dst_ratio = changed_dst_objects / total_dst_objects if total_dst_objects > 0 else 1

        logger.info(f"Ratio of changed src objects: {src_ratio}")
        logger.info(f"Ratio of changed dst objects: {dst_ratio}")

        sum_ratio = src_ratio + dst_ratio
        if sum_ratio >= _FORCE_FULL_RELATE_THRESHOLD:
            logger.info(f"Sum of ratios ({sum_ratio}) exceeds threshold value of {_FORCE_FULL_RELATE_THRESHOLD}. "
                        f"Force full relate.")
            return True

        return False

    def update(self, do_full_update=False):
        self._check_preconditions()

        event_creator = EventCreator(self.dst_has_states)
        initial_load = do_full_update or self._is_initial_load() or self._force_full_relate()

        with ProgressTicker("Process relate src result", 10000) as progress, \
                ContentsWriter() as contents_writer, \
                ContentsWriter() as confirms_writer, \
                EventCollector(contents_writer, confirms_writer, _RELATE_VERSION) as event_collector:

            filename = contents_writer.filename
            confirms = confirms_writer.filename

            result = self._get_updates(initial_load)

            for row in result:
                progress.tick()
                event = event_creator.create_event(self._format_relation(row))
                event_collector.collect(event)

            return filename, confirms

    def get_conflicts(self):
        max_src_event = self._get_max_src_event()
        max_dst_event = self._get_max_dst_event()

        self._get_updates_full(max_src_event, max_dst_event, is_conflicts_query=True)
        yield from self._read_results()
