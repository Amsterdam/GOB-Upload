"""
See README.md in this directory for explanation of this file.

Side notes on this code (JK):
There are some checks in the multiple_allowed code that always compares the source of the relation with something to
decide whether or not to filter on a field (for example the row_number check, or the join on src_dst (that will include
a comparison with dst_id, dst_volgnummer only if multiple_allowed is True for a source. For other sources this check is
neglected). In theory we could simplify the generated query here when for all sources the value for multiple_allowed
is consistent. However, I don't expect that to have any huge impact on the query performance, but it does keep the code
in this class cleaner and more understandable. I would only opt for this query simplification (and thus more complex
code here) if we encounter performance issues with the relate query that are clearly caused by those clauses (after
proper investigation). However, as all this filtering is done on intermediate results and the larger query path itself
shouldn't change between the two variations and the exclusions performed in these steps are minimal, I don't expect any
issues there, and thus I opted for more straightforward code.
"""

import hashlib
import json

from datetime import date, datetime

from gobcore.exceptions import GOBException
from gobcore.events.import_events import ADD, DELETE, CONFIRM, MODIFY
from gobcore.message_broker.offline_contents import ContentsWriter

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
        self.dst_table_name = self.model.get_table_name(self.dst_catalog_name, self.dst_collection_name)

        # Check if source or destination has states (volgnummer, begin_geldigheid, eind_geldigheid)
        self.src_has_states = self.model.has_states(self.src_catalog_name, self.src_collection_name)
        self.dst_has_states = self.model.has_states(self.dst_catalog_name, self.dst_collection_name)

        # Copy relation specs and set default for multiple_allowed
        self.relation_specs = [{
            **spec,
            'multiple_allowed': spec.get('multiple_allowed', False)
        } for spec in self.sources.get_field_relations(src_catalog_name, src_collection_name, src_field_name)]

        if not self.relation_specs:
            raise RelateException("Missing relation specification for " +
                                  f"{src_catalog_name} {src_collection_name} {src_field_name} " +
                                  "(sources.get_field_relations)")

        self.is_many = self.src_field['type'] == "GOB.ManyReference"
        self.relation_table = "rel_" + get_relation_name(self.model, src_catalog_name, src_collection_name,
                                                         src_field_name)

        # Initally don't exclude the relation tables
        self.exclude_relation_table = False

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
        # Switch per source. Different sources have different multiple_allowed values
        whens = [f"WHEN src.{FIELD.APPLICATION} = '{spec['source']}' THEN {self._get_id_for_spec(spec, src_value_ref)}"
                 for spec in self.relation_specs]

        newline = '\n'
        return f"CASE {newline.join(whens)}\nEND"

    def _get_derivation(self):
        """Returns CASE statement for derivation in select expressions.

        :return:
        """
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
            ors = []
            for spec in self.relation_specs:
                or_ = f"src.{FIELD.APPLICATION} = '{spec['source']}'"
                if not spec['multiple_allowed']:
                    or_ += " AND src_dst.row_number = 1"

                ors.append(or_)

            join_on.append(f"(({') OR ('.join(ors)}))")

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
        return self.or_join.join([
            f"({FIELD.APPLICATION} = '{spec['source']}' AND ST_IsValid({spec['source_attribute']}))"
            for spec in self.relation_specs])

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
    LEFT JOIN {self.dst_table_name} dst ON {self.and_join.join(self._src_dst_match())}
    AND dst.{FIELD.DATE_DELETED} IS NULL
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

    def _start_validity_per_seqnr(self, src_or_dst, initial_load=False):
        """Generates the recursive WITH queries that find the begin_geldigheid for every volgnummer

        Result of this recursive query isa relation src_volgnummer_begin_geldigheid or dst_volgnummer_begin_geldigheid
        containing (_id, volgnummer, begin_geldigheid) tuples.

        """
        if src_or_dst == 'src':
            table_name = self.src_table_name
        else:
            table_name = self.dst_table_name

        where_relevant = "TRUE"
        if not initial_load:
            changed_entities = self.src_entities_alias if src_or_dst == 'src' else self.dst_entities_alias
            # Filter the tuples on only the relevant tuples for the update
            where_relevant = f"(_id, volgnummer) in (SELECT _id, volgnummer FROM {changed_entities})"

        return f"""
-- Find all possible {src_or_dst} intervals: id - seqnr - start - end
all_{src_or_dst}_intervals(
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
        {src_or_dst}.{FIELD.SEQNR},
        intv.{FIELD.START_VALIDITY},
        {src_or_dst}.{FIELD.END_VALIDITY}
    FROM all_{src_or_dst}_intervals intv
    LEFT JOIN {table_name} {src_or_dst}
    ON intv.{FIELD.END_VALIDITY} = {src_or_dst}.{FIELD.START_VALIDITY}
        AND {src_or_dst}.{FIELD.ID} = intv.{FIELD.ID}
        AND {src_or_dst}.{FIELD.SEQNR} > intv.{FIELD.SEQNR}
    WHERE {src_or_dst}.{FIELD.START_VALIDITY} IS NOT NULL
),
-- Use these intervals to get all {src_or_dst} start validities:  id - seqnr - start validity
-- Only for the relevant id - seqnr combinations, that is that have changed
-- Or all combinations if it concerns an initial load
{src_or_dst}_volgnummer_begin_geldigheid AS (
    SELECT
        {FIELD.ID},
        {FIELD.SEQNR},
        MIN({FIELD.START_VALIDITY}) {FIELD.START_VALIDITY}
    FROM all_{src_or_dst}_intervals
    WHERE {where_relevant}
    GROUP BY {FIELD.ID}, {FIELD.SEQNR}
)"""

    def _start_validities(self, initial_load=False):
        """Adds recursive queries to determine the begin_geldigheid for each volgnummer

        """
        result = []
        if self.src_has_states:
            result.append(self._start_validity_per_seqnr('src', initial_load))

        if self.dst_has_states:
            result.append(self._start_validity_per_seqnr('dst', initial_load))

        return result

    def _with_src_entities(self):
        filters = []

        if not self.is_many:
            filters.append(f"{self._source_value_ref()} IS NOT NULL")

        if not self.exclude_relation_table:
            filters.append(f"""{FIELD.LAST_EVENT} > (
        SELECT COALESCE(MAX({FIELD.LAST_SRC_EVENT}), 0) FROM {self.relation_table}
    )""")

        filters_str = f'    WHERE {" AND ".join(filters)}' if filters else ""

        statement = f"""
-- All changed source entities
{self.src_entities_alias} AS (
    SELECT * FROM {self.src_table_name} src
{filters_str}
)"""
        return statement

    def _with_dst_entities(self):
        statement = f"""
-- All changed destination entities
{self.dst_entities_alias} AS (
    SELECT * FROM {self.dst_table_name}"""

        if not self.exclude_relation_table:
            statement += f""" WHERE {FIELD.LAST_EVENT} > (
        SELECT COALESCE(MAX({FIELD.LAST_DST_EVENT}), 0) FROM {self.relation_table}
    )"""

        statement += """
)"""

        return statement

    def _with_max_src_event(self):
        return f"""
-- Last event that has updated a source entity
max_src_event AS (SELECT MAX({FIELD.LAST_EVENT}) {FIELD.LAST_EVENT} FROM {self.src_table_name})"""

    def _with_max_dst_event(self):
        return f"""
-- Last event that has updated a destination entity
max_dst_event AS (SELECT MAX({FIELD.LAST_EVENT}) {FIELD.LAST_EVENT} FROM {self.dst_table_name})"""

    def _with_queries(self, initial_load=False):
        start_validities = self._start_validities(initial_load)
        other_withs = [
            self._with_src_entities(),
            self._with_dst_entities(),
            self._with_max_src_event(),
            self._with_max_dst_event()
        ]

        return f"WITH{' RECURSIVE' if start_validities else ''} " \
               f"{','.join(start_validities + other_withs)}\n" \
               f"-- END WITH\n"

    def _join_geldigheid(self, src_dst):
        """Returns the join with the begin_geldigheid for the given volgnummer for either 'src' or 'dst' (src_bg or
        dst_bg)
        """
        return f"LEFT JOIN {src_dst}_volgnummer_begin_geldigheid {src_dst}_bg " \
               f"ON {src_dst}_bg.{FIELD.ID} = {src_dst}.{FIELD.ID} " \
               f"AND {src_dst}_bg.{FIELD.SEQNR} = {src_dst}.{FIELD.SEQNR}"

    def _join_dst_geldigheid(self):
        return self._join_geldigheid('dst') if self.dst_has_states else ""

    def _join_src_geldigheid(self):
        return self._join_geldigheid('src') if self.src_has_states else ""

    def _join_max_event_ids(self):
        return f"""
JOIN max_src_event ON TRUE
JOIN max_dst_event ON TRUE
"""

    def _join_rel_dst_clause(self):
        dst_join_clause = f"rel.dst_id = dst.{FIELD.ID}" + (f" AND rel.dst_volgnummer = dst.{FIELD.SEQNR}"
                                                            if self.dst_has_states else "")
        ors = []
        # Implement conditions
        for spec in self.relation_specs:

            if spec['multiple_allowed']:
                or_ = f"src.{FIELD.APPLICATION} = '{spec['source']}' AND {dst_join_clause}"
            else:
                or_ = f"src.{FIELD.APPLICATION} = '{spec['source']}'"
            ors.append(or_)

        return f" AND (({') OR ('.join(ors)}))"

    def _join_rel(self):
        dst_join = self._join_rel_dst_clause()

        if self.src_has_states:
            return f"""
FULL JOIN (
    SELECT * FROM {self.relation_table}
    WHERE (src_id, src_volgnummer) IN (SELECT {FIELD.ID}, {FIELD.SEQNR} FROM {self.src_entities_alias})
) rel ON rel.src_id = src.{FIELD.ID} AND rel.src_volgnummer = src.{FIELD.SEQNR}
    AND {self._source_value_ref()} = rel.{FIELD.SOURCE_VALUE}
    {dst_join}
"""
        else:
            return f"""
FULL JOIN (
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
        ors = [f"src.{FIELD.APPLICATION} = '{spec['source']}'"
               for spec in self.relation_specs
               if not spec['multiple_allowed']]

        return f"(({') OR ('.join(ors)}) AND row_number > 1)" if ors else 'FALSE'

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
        ors = []

        for spec in self.relation_specs:
            ors.append(
                f"src.{FIELD.APPLICATION} = '{spec['source']}'" +
                (f' AND dst.{FIELD.ID} IS NOT NULL' if spec['multiple_allowed'] else '')
            )

        return f"(({') OR ('.join(ors)}))"

    def get_conflicts_query(self):
        self.exclude_relation_table = True
        return self.get_query()

    def _union_deleted_relations(self, src_or_dst: str):
        assert src_or_dst in ('src', 'dst'), f"src_or_dst should be 'src' or 'dst', not '{src_or_dst}'"

        src_or_dst_entities_alias = self.src_entities_alias if src_or_dst == 'src' else self.dst_entities_alias
        src_or_dst_has_states = self.src_has_states if src_or_dst == 'src' else self.dst_has_states

        in_src_or_dst_entities = f"{src_or_dst}_id IN (SELECT {FIELD.ID} FROM {src_or_dst_entities_alias})" \
            if not src_or_dst_has_states \
            else f"({src_or_dst}_id, {src_or_dst}_volgnummer) IN (SELECT {FIELD.ID}, {FIELD.SEQNR} " \
                 f"FROM {src_or_dst_entities_alias})"

        return f"""
UNION ALL
-- Add all relations for entities in {src_or_dst_entities_alias} that should be deleted
-- These are all current relations that are referenced by {src_or_dst_entities_alias} but are not in {src_or_dst}_side
-- anymore.
SELECT {self.comma_join.join(self._select_expressions_rel_delete())}
FROM {self.relation_table} rel
WHERE {in_src_or_dst_entities} AND rel.id NOT IN (SELECT rel_id FROM {src_or_dst}_side WHERE rel_id IS NOT NULL)
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
    FULL JOIN {self.relation_table} rel
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
""" + (f"""
{self._union_deleted_relations('src') if not self.exclude_relation_table else ''}
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

    def _is_initial_load(self):
        query = f"SELECT {FIELD.ID} FROM {self.relation_table} LIMIT 1"
        result = _execute(query)

        try:
            next(result)
        except StopIteration:
            return True
        return False

    def _check_preconditions(self):
        """Checks if all applications in the src table are defined in GOBSources.

        Relate can't reliably happen if that is not the case.
        :return:
        """

        applications = [spec['source'] for spec in self.relation_specs]

        result = _execute(f"SELECT DISTINCT {FIELD.APPLICATION} FROM {self.src_table_name}")
        src_table_applications = [row[0] for row in result]

        difference = set(src_table_applications) - set(applications)

        if difference:
            raise GOBException(f"Can't relate {self.src_catalog_name} {self.src_collection_name} "
                               f"{self.src_field_name} because the src table contains values for "
                               f"{FIELD.APPLICATION} that are not defined in GOBSources: {','.join(difference)}")

    def update(self):
        self._check_preconditions()

        initial_load = self._is_initial_load()
        query = self.get_query(initial_load)

        with ProgressTicker("Process relate src result", 10000) as progress, \
                ContentsWriter() as contents_writer, \
                ContentsWriter() as confirms_writer, \
                EventCollector(contents_writer, confirms_writer) as event_collector:

            filename = contents_writer.filename
            confirms = confirms_writer.filename

            try:
                # Execute the query in a try-except block.
                # The query is complex and if it fails it is hard to debug
                # In order to allow debugging, any failing query is reported on stdout
                # Afterwards the exception is re-raised
                result = _execute(query, stream=True, max_row_buffer=25000)
                for row in result:
                    progress.tick()
                    event = self._create_event(self._format_relation(dict(row)))
                    event_collector.collect(event)
                result.close()
            except Exception as e:
                print(f"Update failed: {str(e)}, Failing query:\n{query}\n")
                raise e

            return filename, confirms
