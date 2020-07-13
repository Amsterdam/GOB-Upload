"""
See README.md in this directory for explanation of this file.
"""

import hashlib
import json
import random

from datetime import date, datetime
from typing import List

from gobcore.exceptions import GOBException
from gobcore.events.import_events import ADD, DELETE, CONFIRM, MODIFY
from gobcore.message_broker.offline_contents import ContentsWriter

from gobcore.logging.logger import logger
from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobcore.model.relations import get_relation_name
from gobcore.sources import GOBSources
from gobcore.utils import ProgressTicker
from gobcore.typesystem.json import GobTypeJSONEncoder

from gobupload.relate.exceptions import RelateException
from gobupload.storage.execute import _execute
from gobupload.compare.event_collector import EventCollector

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


class Relater:
    model = GOBModel()
    sources = GOBSources()

    space_join = ' \n    '
    comma_join = ',\n    '
    and_join = ' AND\n    '
    or_join = ' OR\n    '

    json_join_alias = 'json_arr_elm'
    src_entities_alias = 'src_entities'
    dst_entities_alias = 'dst_entities'

    # The names of the fields to be returned. Optionally extendes with src_volgnummer and/or dst_volgnummer if
    # applicable
    select_aliases = [
        FIELD.VERSION,
        FIELD.APPLICATION,
        FIELD.SOURCE_ID,
        FIELD.SOURCE,
        FIELD.EXPIRATION_DATE,
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

        self.src_collection = self.model.get_collection(src_catalog_name, src_collection_name)
        self.src_field = self.src_collection['all_fields'].get(src_field_name)
        self.src_table_name = self.model.get_table_name(src_catalog_name, src_collection_name)

        # Get the destination catalog and collection names
        self.dst_catalog_name, self.dst_collection_name = self.src_field['ref'].split(':')
        self.dst_collection = self.model.get_collection(self.dst_catalog_name, self.dst_collection_name)
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

        # Initally don't exclude the relation tables
        self.exclude_relation_table = False

        self.min_src_event_id = 0
        self.max_src_event_id = None

        # begin_geldigheid tmp table names
        datestr = datetime.now().strftime('%Y%m%d')
        self.src_intv_tmp_table_name = f"tmp_{self.src_catalog_name}_{self.src_collection['abbreviation']}_intv_" \
                                       f"{datestr}_{str(random.randint(0, 1000)).zfill(4)}".lower()
        self.dst_intv_tmp_table_name = f"tmp_{self.dst_catalog_name}_{self.dst_collection['abbreviation']}_intv_" \
                                       f"{datestr}_{str(random.randint(0, 1000)).zfill(4)}".lower()

    def _get_applications_in_src(self):
        query = f"SELECT DISTINCT {FIELD.APPLICATION} FROM {self.src_table_name}"

        result = _execute(query)
        return [row[0] for row in result]

    def _validity_select_expressions(self):
        if self.src_has_states and self.dst_has_states:
            start = f"GREATEST(src_bg.{FIELD.START_VALIDITY}, dst_bg.{FIELD.START_VALIDITY})"
            end = f"LEAST(src.{FIELD.END_VALIDITY}, dst.{FIELD.END_VALIDITY})"
        elif self.src_has_states:
            start = f"src_bg.{FIELD.START_VALIDITY}"
            end = f"src.{FIELD.END_VALIDITY}"
        elif self.dst_has_states:
            start = f"dst_bg.{FIELD.START_VALIDITY}"
            end = f"dst.{FIELD.END_VALIDITY}"
        else:
            start = "NULL"
            end = "NULL"

        return start, end

    def _select_aliases(self):
        return self.select_aliases + (self.select_relation_aliases if not self.exclude_relation_table else []) \
                  + (['src_volgnummer'] if self.src_has_states else []) \
                  + (['dst_volgnummer'] if self.dst_has_states else [])

    def _build_select_expressions(self, mapping: dict, exclude_aliases: list=[]):
        aliases = self._select_aliases()

        aliases = [i for i in aliases if i not in exclude_aliases]

        assert all([alias in mapping.keys() for alias in aliases]), \
            'Missing key(s): ' + str([alias for alias in aliases if alias not in mapping.keys()])

        return [f'{mapping[alias]} AS {alias}' for alias in aliases]

    def _select_expressions_dst(self):
        start_validity, end_validity = self._validity_select_expressions()

        mapping = {
            FIELD.VERSION: f"src.{FIELD.VERSION}",
            FIELD.APPLICATION: f"src.{FIELD.APPLICATION}",
            FIELD.SOURCE_ID: self._get_id_for_dst(),
            FIELD.SOURCE: f"'{GOB}'",
            FIELD.EXPIRATION_DATE: f"LEAST(src.{FIELD.EXPIRATION_DATE}, dst.{FIELD.EXPIRATION_DATE})",
            "id": self._get_id_for_dst(),
            "derivation": self._get_derivation(),
            "src_source": f"src.{FIELD.SOURCE}",
            "src_id": f"src.{FIELD.ID}",
            "src_volgnummer": f"src.{FIELD.SEQNR}",
            "src_last_event": f"src.{FIELD.LAST_EVENT}",
            "dst_source": f"CASE WHEN dst.{FIELD.DATE_DELETED} IS NULL THEN dst.{FIELD.SOURCE} ELSE NULL END",
            "dst_id": f"CASE WHEN dst.{FIELD.DATE_DELETED} IS NULL THEN dst.{FIELD.ID} ELSE NULL END",
            "dst_volgnummer": f"CASE WHEN dst.{FIELD.DATE_DELETED} IS NULL THEN dst.{FIELD.SEQNR} ELSE NULL END",
            FIELD.SOURCE_VALUE: f"src.{FIELD.SOURCE_VALUE}",
            FIELD.LAST_SRC_EVENT: f"max_src_event.{FIELD.LAST_EVENT}",
            FIELD.LAST_DST_EVENT: f"max_dst_event.{FIELD.LAST_EVENT}",
            FIELD.LAST_EVENT: f"rel.{FIELD.LAST_EVENT}",
            "src_deleted": "NULL::timestamp without time zone",
            "rel_deleted": f"rel.{FIELD.DATE_DELETED}",
            "rel_id": "rel.id",
            "rel_dst_id": "rel.dst_id",
            "rel_dst_volgnummer": "rel.dst_volgnummer",
            f"rel_{FIELD.EXPIRATION_DATE}": f"rel.{FIELD.EXPIRATION_DATE}",
            f"rel_{FIELD.START_VALIDITY}": f"rel.{FIELD.START_VALIDITY}",
            f"rel_{FIELD.END_VALIDITY}": f"rel.{FIELD.END_VALIDITY}",
            f"rel_{FIELD.HASH}": f"rel.{FIELD.HASH}",
            FIELD.START_VALIDITY: f"CASE WHEN dst.{FIELD.ID} IS NULL THEN NULL ELSE {start_validity} END",
            FIELD.END_VALIDITY: f"CASE WHEN dst.{FIELD.ID} IS NULL THEN NULL ELSE {end_validity} END",
        }
        return self._build_select_expressions(mapping, exclude_aliases=["row_number"])

    def _select_expressions_src(self, exclude_relation_aliases=False):
        """Returns the select expressions for the src query

        :return:
        """
        start_validity, end_validity = self._validity_select_expressions()

        mapping = {
            FIELD.VERSION: f"src.{FIELD.VERSION}",
            FIELD.APPLICATION: f"src.{FIELD.APPLICATION}",
            FIELD.SOURCE_ID: self._get_id(),
            FIELD.SOURCE: f"'{GOB}'",
            FIELD.EXPIRATION_DATE: f"LEAST(src.{FIELD.EXPIRATION_DATE}, dst.{FIELD.EXPIRATION_DATE})",
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
            FIELD.LAST_SRC_EVENT: f"max_src_event.{FIELD.LAST_EVENT}",
            FIELD.LAST_DST_EVENT: f"max_dst_event.{FIELD.LAST_EVENT}",
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
            FIELD.START_VALIDITY: f"CASE WHEN dst.{FIELD.ID} IS NULL THEN NULL ELSE {start_validity} END",
            FIELD.END_VALIDITY: f"CASE WHEN dst.{FIELD.ID} IS NULL THEN NULL ELSE {end_validity} END",
            "row_number": "row_number",
        }

        select_expressions = self._build_select_expressions(mapping)

        return select_expressions

    def _select_expressions_rel_delete(self):
        mapping = {alias: 'NULL' for alias in self._select_aliases()}
        mapping.update({
            FIELD.LAST_EVENT: f"rel.{FIELD.LAST_EVENT}",
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
        elif len(self.relation_specs) == 1:
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
        elif len(self.relation_specs) == 1:
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
        else:
            return f"src.{self.src_field_name}->>'{FIELD.SOURCE_VALUE}'"

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
        else:
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
            clause.extend([
                f"(dst.{FIELD.START_VALIDITY} < src.{FIELD.END_VALIDITY} "
                f"OR src.{FIELD.END_VALIDITY} IS NULL)",
                f"(dst.{FIELD.END_VALIDITY} >= src.{FIELD.END_VALIDITY} "
                f"OR dst.{FIELD.END_VALIDITY} IS NULL)"
            ])
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

        if not self.exclude_relation_table:
            # Add check for row_number for all sources that don't have multiple_allowed set.

            join_on.append(self._switch_for_specs(
                'multiple_allowed',
                lambda spec: 'src_dst.row_number = 1' if not spec['multiple_allowed'] else 'TRUE'
            ))

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
        return any([spec['method'] != EQUALS for spec in self.relation_specs])

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

    def _src_dst_select(self):
        return f"SELECT * FROM {self.src_entities_alias} WHERE {FIELD.DATE_DELETED} IS NULL"

    def _src_dst_join(self):
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
LEFT JOIN (
    SELECT
        {self.comma_join.join(self._src_dst_select_expressions())}
    FROM (
        {self._src_dst_select()}
    ) src
    {validgeo_src}
    {self._join_array_elements() if self.is_many else ""}
    LEFT JOIN {self.dst_table_name} dst ON {self.and_join.join(join_on)}
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
        {self._source_value_ref()} {FIELD.SOURCE_VALUE}
    FROM {self.src_table_name} src
    {self._join_array_elements() if self.is_many else ""}
    WHERE src.{FIELD.DATE_DELETED} IS NULL AND ({','.join(not_in_fields)}) NOT IN (
        SELECT {','.join(not_in_fields)} FROM {self.src_entities_alias}
    ){source_value_not_null}
"""

    def _start_validity_per_seqnr(self, table_name: str):
        """Generates the recursive WITH queries that find the begin_geldigheid for every volgnummer

        Result of this recursive query isa relation src_volgnummer_begin_geldigheid or dst_volgnummer_begin_geldigheid
        containing (_id, volgnummer, begin_geldigheid) tuples.

        """

        return f"""
-- Find all possible intervals for {table_name}: id - seqnr - start - end
WITH RECURSIVE
all_intervals(
    {FIELD.ID},
    start_{FIELD.SEQNR},
    {FIELD.SEQNR},
    {FIELD.START_VALIDITY},
    {FIELD.END_VALIDITY}) AS (
    SELECT
        s.{FIELD.ID},
        s.{FIELD.SEQNR},
        s.{FIELD.SEQNR},
        s.{FIELD.START_VALIDITY},
        s.{FIELD.END_VALIDITY}
    FROM {table_name} s
    LEFT JOIN {table_name} t
    ON s.{FIELD.ID} = t.{FIELD.ID}
        AND t.{FIELD.SEQNR} < s.{FIELD.SEQNR}
        AND t.{FIELD.END_VALIDITY} = s.{FIELD.START_VALIDITY}
    WHERE t.{FIELD.ID} IS NULL
    UNION
    SELECT
        intv.{FIELD.ID},
        intv.start_{FIELD.SEQNR},
        t.{FIELD.SEQNR},
        intv.{FIELD.START_VALIDITY},
        t.{FIELD.END_VALIDITY}
    FROM all_intervals intv
    LEFT JOIN {table_name} t
    ON intv.{FIELD.END_VALIDITY} = t.{FIELD.START_VALIDITY}
        AND t.{FIELD.ID} = intv.{FIELD.ID}
        AND t.{FIELD.SEQNR} > intv.{FIELD.SEQNR}
    WHERE t.{FIELD.START_VALIDITY} IS NOT NULL
)
SELECT
    {FIELD.ID},
    {FIELD.SEQNR},
    MIN({FIELD.START_VALIDITY}) {FIELD.START_VALIDITY}
FROM all_intervals
GROUP BY {FIELD.ID}, {FIELD.SEQNR}
"""

    def _cleanup_tmp_tables(self):
        _execute(f"DROP TABLE IF EXISTS {self.src_intv_tmp_table_name}")
        _execute(f"DROP TABLE IF EXISTS {self.dst_intv_tmp_table_name}")

    def _create_tmp_tables(self):
        """Creates tmp tables from recursive queries to determine the begin_geldigheid for each volgnummer

        """
        self._cleanup_tmp_tables()

        if self.src_has_states:
            query = self._start_validity_per_seqnr(self.src_table_name)

            logger.info(f"Creating temporary table {self.src_intv_tmp_table_name}")
            _execute(f"CREATE TABLE IF NOT EXISTS {self.src_intv_tmp_table_name} AS ({query})")
            _execute(f"CREATE INDEX ON {self.src_intv_tmp_table_name}({FIELD.ID}, {FIELD.SEQNR})")

        if self.dst_has_states:
            if self.src_table_name == self.dst_table_name:
                # Use same table as src as they are the same
                self.dst_intv_tmp_table_name = self.src_intv_tmp_table_name
            else:
                query = self._start_validity_per_seqnr(self.dst_table_name)

                logger.info(f"Creating temporary table {self.dst_intv_tmp_table_name}")
                _execute(f"CREATE TABLE IF NOT EXISTS {self.dst_intv_tmp_table_name} AS ({query})")
                _execute(f"CREATE INDEX ON {self.dst_intv_tmp_table_name}({FIELD.ID}, {FIELD.SEQNR})")

    def _changed_source_ids(self, catalogue: str, collection: str, last_eventid: str, related_attributes: List[str]):
        """Returns a query that returns the source_ids from all events after last_eventid that are interesting for the
        relate. These include all source_ids associated with ADD or DELETE events and all MODIFY events that include
        modifications on columns such as START_VALIDITY, END_VALIDITY, EXPIRATION_DATE plus the attributes that are
        used in the relate process (provided in the related_attributes list).

        The last_eventid parameter may be any value (an integer string, but also a query that determines the
        last_eventid)

        :param catalogue:
        :param collection:
        :param last_eventid:
        :param related_attributes:
        :return:
        """
        interesting_modifications = [FIELD.START_VALIDITY, FIELD.END_VALIDITY, FIELD.SOURCE] + related_attributes
        keys = ", ".join([f"'{field}'" for field in interesting_modifications])

        return f"""
SELECT e.source_id
FROM events e
INNER JOIN jsonb_array_elements(e.contents -> 'modifications') modifications
ON modifications ->> 'key' IN ({keys})
WHERE catalogue = '{catalogue}'
  AND entity = '{collection}'
  AND eventid > {last_eventid}
  AND action = '{MODIFY.name}'
UNION
SELECT e.source_id
FROM events e
WHERE catalogue = '{catalogue}'
  AND entity = '{collection}'
  AND eventid > {last_eventid}
  AND action IN ('{ADD.name}', '{DELETE.name}')
"""

    def _src_entities(self, initial_load=False):
        limit = ''
        filters = []

        if not self.is_many:
            filters.append(f"{self._source_value_ref()} IS NOT NULL")

        # min_src_event_id and max_src_event_id are used for pagination during the initial load.
        if self.min_src_event_id:
            filters.append(f"src.{FIELD.LAST_EVENT} > {self.min_src_event_id}")

        if self.max_src_event_id:
            filters.append(f"src.{FIELD.LAST_EVENT} <= {self.max_src_event_id}")

        if not self.exclude_relation_table and not initial_load:
            last_eventid = f"(SELECT COALESCE(MAX({FIELD.LAST_SRC_EVENT}), 0) FROM {self.relation_table})"
            source_attrs = [spec['source_attribute'] for spec in self.relation_specs if 'source_attribute' in spec]
            changed_source_ids = self._changed_source_ids(
                self.src_catalog_name,
                self.src_collection_name,
                last_eventid,
                [self.src_field_name] + source_attrs
            )

            filters.append(f"src.{FIELD.SOURCE_ID} IN ({changed_source_ids})")
        elif not self.exclude_relation_table and initial_load:
            # Initial load. Limit the number of src entities. Only possible for initial_load!
            limit = f'ORDER BY src.{FIELD.LAST_EVENT} LIMIT {_MAX_ROWS_PER_SIDE}'

        filters_str = f'WHERE {" AND ".join(filters)}' if filters else ""

        return f"""
SELECT * FROM {self.src_table_name} src
{filters_str}
{limit}
"""

    def _with_src_entities(self, initial_load=False):
        statement = f"""
-- All changed source entities
{self.src_entities_alias} AS ({self._src_entities(initial_load)})"""
        return statement

    def _dst_entities(self):
        query = f"""SELECT * FROM {self.dst_table_name}"""

        if not self.exclude_relation_table:
            last_eventid = f"(SELECT COALESCE(MAX({FIELD.LAST_DST_EVENT}), 0) FROM {self.relation_table})"
            changed_source_ids = self._changed_source_ids(
                self.dst_catalog_name,
                self.dst_collection_name,
                last_eventid,
                [spec['destination_attribute'] for spec in self.relation_specs]
            )
            query += f" WHERE {FIELD.SOURCE_ID} IN ({changed_source_ids})"
        return query

    def _with_dst_entities(self):
        statement = f"""
-- All changed destination entities
{self.dst_entities_alias} AS ({self._dst_entities()})"""

        return statement

    def _get_count_for(self, frm: str):
        result = _execute(f"SELECT COUNT(*) FROM {frm} alias")
        return next(result)[0]

    def _with_max_src_event(self):
        if self.max_src_event_id:
            return f"""
-- Last event we're considering in this relate process
max_src_event AS (SELECT {self.max_src_event_id} {FIELD.LAST_EVENT})"""

        else:
            return f"""
-- Last event that has updated a source entity
max_src_event AS (SELECT MAX({FIELD.LAST_EVENT}) {FIELD.LAST_EVENT} FROM {self.src_table_name})"""

    def _with_max_dst_event(self):
        return f"""
-- Last event that has updated a destination entity
max_dst_event AS (SELECT MAX({FIELD.LAST_EVENT}) {FIELD.LAST_EVENT} FROM {self.dst_table_name})"""

    def _with_queries(self, initial_load=False):
        withs = [
            self._with_src_entities(initial_load),
            self._with_max_src_event(),
            self._with_max_dst_event()
        ]

        if not initial_load:
            withs.append(self._with_dst_entities())

        return f"WITH " \
               f"{','.join(withs)}\n" \
               f"-- END WITH\n"

    def _join_geldigheid(self, table_name: str, alias: str, join_with: str):
        """Returns the join with the begin_geldigheid for the given volgnummer for either 'src' or 'dst' (src_bg or
        dst_bg)
        """
        return f"LEFT JOIN {table_name} {alias} " \
               f"ON {alias}.{FIELD.ID} = {join_with}.{FIELD.ID} " \
               f"AND {alias}.{FIELD.SEQNR} = {join_with}.{FIELD.SEQNR}"

    def _join_dst_geldigheid(self):
        return self._join_geldigheid(self.dst_intv_tmp_table_name, 'dst_bg', 'dst') if self.dst_has_states else ""

    def _join_src_geldigheid(self):
        return self._join_geldigheid(self.src_intv_tmp_table_name, 'src_bg', 'src') if self.src_has_states else ""

    def _join_max_event_ids(self):
        return f"""
JOIN max_src_event ON TRUE
JOIN max_dst_event ON TRUE
"""

    def _join_rel_dst_clause(self):
        dst_join_clause = f"rel.dst_id = dst.{FIELD.ID}" + (f" AND rel.dst_volgnummer = dst.{FIELD.SEQNR}"
                                                            if self.dst_has_states else "")

        switch = self._switch_for_specs(
            'multiple_allowed',
            lambda spec: dst_join_clause if spec['multiple_allowed'] else 'TRUE'
        )
        return f" AND {switch}"

    def _join_rel(self):
        dst_join = self._join_rel_dst_clause()

        if self.src_has_states:
            return f"""
LEFT JOIN (
    SELECT * FROM {self.relation_table}
    WHERE (src_id, src_volgnummer) IN (SELECT {FIELD.ID}, {FIELD.SEQNR} FROM {self.src_entities_alias})
) rel ON rel.src_id = src.{FIELD.ID} AND rel.src_volgnummer = src.{FIELD.SEQNR}
    AND {self._source_value_ref()} = rel.{FIELD.SOURCE_VALUE}
    {dst_join}
"""
        else:
            return f"""
LEFT JOIN (
    SELECT * FROM {self.relation_table}
    WHERE src_id IN (SELECT {FIELD.ID} FROM {self.src_entities_alias})
) rel ON rel.src_id = src.{FIELD.ID} AND {self._source_value_ref()} = rel.{FIELD.SOURCE_VALUE}
    {dst_join}
"""

    def _filter_conflicts(self):
        """Returns row_number check per source. Skip sources that have multiple_allowed set to True.
        Only used in the conflicts query.

        :return:
        """

        return self._switch_for_specs(
            'multiple_allowed',
            lambda spec: f'row_number > 1' if not spec['multiple_allowed'] else 'FALSE'
        )

    def _get_where(self):
        """Returns WHERE clause for both sides of the UNION.
        Only include not-deleted relations or existing sources.

        Reason is that sometimes we do need deleted relations, in case the src is matched with a previously deleted
        row in the relation table. We will need the last event of that row to re-add that row.

        :return:
        """
        has_multiple_allowed_source = any([spec['multiple_allowed'] for spec in self.relation_specs])
        return f"WHERE (" \
               + (f"rel.{FIELD.DATE_DELETED} IS NULL OR " if not self.exclude_relation_table else "") \
               + f"src.{FIELD.ID} IS NOT NULL)" \
               + f" AND dst.{FIELD.DATE_DELETED} IS NULL" \
               + (f" AND {self._filter_conflicts()}" if self.exclude_relation_table else "") \
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
        else:
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

    def _get_conflicts_query(self):
        self.exclude_relation_table = True
        return self.get_query()

    def get_conflicts(self):
        self._prepare_query()
        query = self._get_conflicts_query()

        yield from _execute(query, stream=True, max_row_buffer=25000)

        self._cleanup()

    def _union_deleted_relations(self, src_or_dst: str):
        assert src_or_dst in ('src', 'dst'), f"src_or_dst should be 'src' or 'dst', not '{src_or_dst}'"

        src_or_dst_entities_alias = self.src_entities_alias if src_or_dst == 'src' else self.dst_entities_alias
        src_or_dst_has_states = self.src_has_states if src_or_dst == 'src' else self.dst_has_states

        in_src_or_dst_entities = f"{src_or_dst}_id IN (SELECT {FIELD.ID} FROM {src_or_dst_entities_alias})" \
            if not src_or_dst_has_states \
            else f"({src_or_dst}_id, {src_or_dst}_volgnummer) IN (SELECT {FIELD.ID}, {FIELD.SEQNR} " \
                 f"FROM {src_or_dst_entities_alias})"

        # rel_id should not be in src_side and dst_side
        rel_ids = "SELECT rel_id FROM src_side WHERE rel_id IS NOT NULL" + \
                  (" UNION ALL SELECT rel_id FROM dst_side WHERE rel_id IS NOT NULL" if src_or_dst == 'dst' else "")

        return f"""
UNION ALL
-- Add all relations for entities in {src_or_dst_entities_alias} that should be deleted
-- These are all current relations that are referenced by {src_or_dst_entities_alias} but are not in {src_or_dst}_side
-- anymore.
SELECT {self.comma_join.join(self._select_expressions_rel_delete())}
FROM {self.relation_table} rel
WHERE {in_src_or_dst_entities} AND rel.id NOT IN ({rel_ids})
    AND rel.{FIELD.DATE_DELETED} IS NULL
"""

    def get_query(self, initial_load=False):
        """Builds and returns the event extraction query

        Omits right-hand side of the query when initial_load=True, because it
        is unnecessary, and excluding everything on the right-hand side that is done on the left-hand-side creates
        a heavy query.

        When self.exclude_relation_table=True, the relation table will not be joined and only conflicts will
        be returned.
        :return:
        """

        dst_table_outer_join_on = self._dst_table_outer_join_on()

        relate_dst_side = not initial_load and not self.exclude_relation_table

        query = f"""
{self._with_queries(initial_load)},
src_side AS (
    -- Relate all changed src entities
    SELECT
        {self.comma_join.join(self._select_expressions_src())}
    FROM {self.src_entities_alias} src
    {self._join_array_elements() if self.is_many else ""}
    {self._src_dst_join()}
    LEFT JOIN {self.dst_table_name} dst
        ON {self.and_join.join(dst_table_outer_join_on)}
    {self._join_rel() if not self.exclude_relation_table else ""}
    {self._join_src_geldigheid()}
    {self._join_dst_geldigheid()}
    {self._join_max_event_ids()}
    {self._get_where()}
)
"""

        if relate_dst_side:
            query += f""",
dst_side AS (
    -- Relate all changed dst entities, but exclude relations that are also related in src_side
    SELECT
        {self.comma_join.join(self._select_aliases())}
    FROM (
    SELECT
        {self.comma_join.join(self._select_expressions_dst())},
        {self._row_number_partition(f'src.{FIELD.SOURCE_VALUE}')} AS row_number
    FROM {self.dst_entities_alias} dst
    INNER JOIN ({self._select_rest_src()}) src
        ON {self.and_join.join(self._src_dst_match(f'src.{FIELD.SOURCE_VALUE}'))}
    LEFT JOIN {self.relation_table} rel
        ON rel.src_id=src.{FIELD.ID} {f'AND rel.src_volgnummer = src.{FIELD.SEQNR}' if self.src_has_states else ''}
        AND rel.src_source = src.{FIELD.SOURCE} AND rel.{FIELD.SOURCE_VALUE} = src.{FIELD.SOURCE_VALUE}
        {self._join_rel_dst_clause()}
    {self._join_src_geldigheid()}
    {self._join_dst_geldigheid()}
    {self._join_max_event_ids()}
    {self._get_where()}
    ) q
)
"""

        query += f"""
-- All relations for changed src entities
SELECT * FROM src_side
""" + \
                 (self._union_deleted_relations('src')
                  if not self.exclude_relation_table
                  else '') + (f"""
UNION ALL
-- All relations for changed dst entities
SELECT * FROM dst_side
{self._union_deleted_relations('dst')}
""" if relate_dst_side else "")

        return query

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

    def _create_event(self, row: dict):
        compare_fields = [
            'dst_id',
            FIELD.EXPIRATION_DATE,
            FIELD.START_VALIDITY,
            FIELD.END_VALIDITY,
        ]

        if self.dst_has_states:
            compare_fields.append('dst_volgnummer')

        if row['rel_id'] is None or row['rel_deleted'] is not None:
            # No relation yet, or previously deleted relation. Create ADD
            ignore_fields = [
                'src_deleted',
                'rel_deleted',
                'src_last_event',
                'rel_id',
                f'rel_{FIELD.HASH}',
                'rel_dst_volgnummer',
            ] + [f"rel_{field}" for field in compare_fields]

            data = {k: v for k, v in row.items() if k not in ignore_fields}
            return ADD.create_event(row[FIELD.SOURCE_ID], row[FIELD.SOURCE_ID], data)
        elif row['src_deleted'] is not None or row['src_id'] is None:
            data = {FIELD.LAST_EVENT: row[FIELD.LAST_EVENT]}
            return DELETE.create_event(row['rel_id'], row['rel_id'], data)
        else:
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
                return MODIFY.create_event(row['rel_id'], row['rel_id'], data)
            else:
                data = {FIELD.LAST_EVENT: row[FIELD.LAST_EVENT]}
                return CONFIRM.create_event(row['rel_id'], row['rel_id'], data)

    def _format_relation(self, relation: dict):
        timestamps = [FIELD.START_VALIDITY, FIELD.END_VALIDITY, FIELD.EXPIRATION_DATE]

        # Add time-part to date objects
        relation.update({
            field: datetime.combine(relation.get(field), datetime.min.time())
            for field in timestamps
            if relation.get(field) and isinstance(relation.get(field), date)
        })

        # Remove row_number from the relation
        relation.pop('row_number', None)

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

    def _force_full_relate(self) -> bool:
        """Returns True if should force full relate. Full relate is forced when there are many updated src/dst objects
        to consider.

        :return:
        """
        total_src_objects = self._get_count_for(self.src_table_name)
        total_dst_objects = self._get_count_for(self.dst_table_name)

        changed_src_objects = self._get_count_for(f'({self._src_entities()})')
        changed_dst_objects = self._get_count_for(f'({self._dst_entities()})')

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

    def _get_max_src_event(self):
        query = f"SELECT MAX({FIELD.LAST_EVENT}) FROM {self.src_table_name}"
        return next(_execute(query))[0]

    def _get_paged_updates(self):
        """Paged updates are always when initial_load is True

        :return:
        """

        # Set a max to the _last_event we consider for the src entities. If we don't do this, we risk considering
        # certain src entities twice when a src entity is updated while this job is running.
        self.max_src_event_id = self._get_max_src_event()

        while True:
            min_src_event_id_before = self.min_src_event_id

            result = self._query_results(True)

            for row in result:
                # row['src_last_event'] may be None when the relation table isn't empty: a row from the relation table
                # is found that does not match any src objects, meaning this row will be deleted. Set to 0
                self.min_src_event_id = max(self.min_src_event_id, row['src_last_event'] or 0)
                yield row

            if min_src_event_id_before == self.min_src_event_id:
                # No updates
                break

    def _prepare_query(self):
        """Prepare step before the query can be executed

        :return:
        """
        logger.info("Create temporary tables.")
        self._create_tmp_tables()

    def _cleanup(self):
        logger.info("Removing temporary tables")
        self._cleanup_tmp_tables()

    def _get_updates(self, initial_load):
        """Return updates from the database.

        If this is an initial_load, paginate the results by default. Pagination is only possible for initial loads.

        :param initial_load:
        :return:
        """
        self._prepare_query()

        if initial_load:
            logger.info("Initial load. Relate in batches.")
            yield from self._get_paged_updates()
        else:
            logger.info("Update run. Relate in once.")
            yield from self._query_results(initial_load)

        self._cleanup()

    def _query_results(self, initial_load):
        query = self.get_query(initial_load)

        # Execute the query in a try-except block.
        # The query is complex and if it fails it is hard to debug
        # In order to allow debugging, any failing query is reported on stdout
        # Afterwards the exception is re-raised
        try:
            result = _execute(query, stream=True, max_row_buffer=25000)
            for row in result:
                yield dict(row)
            result.close()
        except Exception as e:
            print(f"Update failed: {str(e)}, Failing query:\n{query}\n")
            raise e

    def update(self, do_full_update=False):
        self._check_preconditions()

        initial_load = do_full_update or self._is_initial_load() or self._force_full_relate()

        with ProgressTicker("Process relate src result", 10000) as progress, \
                ContentsWriter() as contents_writer, \
                ContentsWriter() as confirms_writer, \
                EventCollector(contents_writer, confirms_writer) as event_collector:

            filename = contents_writer.filename
            confirms = confirms_writer.filename

            result = self._get_updates(initial_load)

            for row in result:
                progress.tick()
                event = self._create_event(self._format_relation(row))
                event_collector.collect(event)

            return filename, confirms
