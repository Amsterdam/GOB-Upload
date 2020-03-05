"""
See README.md in this directory for explanation of this file.

"""
import hashlib
import json

from datetime import date, datetime

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


class RelationTableRelater:
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
        FIELD.LAST_EVENT,
        FIELD.START_VALIDITY,
        FIELD.END_VALIDITY,
        'src_deleted',
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

        self.relation_specs = self.sources.get_field_relations(src_catalog_name, src_collection_name, src_field_name)
        if not self.relation_specs:
            raise RelateException("Missing relation specification for " +
                                  f"{src_catalog_name} {src_collection_name} {src_field_name} " +
                                  "(sources.get_field_relations)")

        self.is_many = self.src_field['type'] == "GOB.ManyReference"
        self.relation_table = "rel_" + get_relation_name(self.model, src_catalog_name, src_collection_name,
                                                         src_field_name)

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
        return self.select_aliases + (['src_volgnummer'] if self.src_has_states else []) \
                  + (['dst_volgnummer'] if self.dst_has_states else [])

    def _build_select_expressions(self, mapping: dict):
        aliases = self._select_aliases()

        assert all([alias in mapping.keys() for alias in aliases]), \
            'Missing key(s): ' + str([alias for alias in aliases if alias not in mapping.keys()])

        return [f'{mapping[alias]} AS {alias}' for alias in aliases]

    def _select_expressions_dst(self):
        start_validity, end_validity = self._validity_select_expressions()

        mapping = {
            FIELD.VERSION: f"rel.{FIELD.VERSION}",
            FIELD.APPLICATION: f"rel.{FIELD.APPLICATION}",
            FIELD.SOURCE_ID: f"rel.{FIELD.SOURCE_ID}",
            FIELD.SOURCE: f"rel.{FIELD.SOURCE}",
            FIELD.EXPIRATION_DATE: f"LEAST(src.{FIELD.EXPIRATION_DATE}, dst.{FIELD.EXPIRATION_DATE})",
            "id": f"rel.id",
            "derivation": "rel.derivation",
            "src_source": "rel.src_source",
            "src_id": "rel.src_id",
            "src_volgnummer": "rel.src_volgnummer",
            "dst_source": f"CASE WHEN dst.{FIELD.DATE_DELETED} IS NULL THEN dst.{FIELD.SOURCE} ELSE NULL END",
            "dst_id": f"CASE WHEN dst.{FIELD.DATE_DELETED} IS NULL THEN dst.{FIELD.ID} ELSE NULL END",
            "dst_volgnummer": f"CASE WHEN dst.{FIELD.DATE_DELETED} IS NULL THEN dst.{FIELD.SEQNR} ELSE NULL END",
            FIELD.SOURCE_VALUE: f"rel.{FIELD.SOURCE_VALUE}",
            FIELD.LAST_SRC_EVENT: f"max_src_event.{FIELD.LAST_EVENT}",
            FIELD.LAST_DST_EVENT: f"max_dst_event.{FIELD.LAST_EVENT}",
            FIELD.LAST_EVENT: f"rel.{FIELD.LAST_EVENT}",
            "src_deleted": "NULL::timestamp without time zone",
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
        return self._build_select_expressions(mapping)

    def _select_expressions_src(self):
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

        return self._build_select_expressions(mapping)

    def _get_id(self):
        id_fields = [f"src.{FIELD.ID}"]

        if self.src_has_states:
            id_fields += [f"src.{FIELD.SEQNR}"]

        id_fields += [
            f"src.{FIELD.SOURCE}",
            f"({self._source_value_ref()})"
        ]
        return " || '.' || ".join(id_fields)

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
        source_value_ref = source_value_ref if source_value_ref is not None \
            else f"{self._json_obj_ref()}->>'bronwaarde'"

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
            f"src_dst.row_number = 1"
        ]

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

    def _row_number_partition(self):
        """Returns the row_number() window function to avoid duplicate (src_id, [src_volgummer,] bronwaarde) rows in
        the relation table. Partitions by src_id, src_volgnummer and bronwaarde, so that we can select only the row
        where the row number = 1. Rows within a partition are ordered by bronwaarde.

        :return:
        """
        partition_by = [
            f"src.{FIELD.ID}",
            f"src.{FIELD.SEQNR}",
            self._source_value_ref()
        ] if self.src_has_states else [
            f"src.{FIELD.ID}",
            self._source_value_ref()
        ]

        return f"row_number() OVER (PARTITION BY {','.join(partition_by)} ORDER BY {self._source_value_ref()})"

    def _select_rest_src(self):
        not_in_fields = [FIELD.ID, FIELD.SEQNR] if self.src_has_states else [FIELD.ID]

        return f"""
    SELECT
        src.*,
        {self._source_value_ref()} {FIELD.SOURCE_VALUE}
    FROM {self.src_table_name} src
    {self._join_array_elements() if self.is_many else ""}
    WHERE src.{FIELD.DATE_DELETED} IS NULL AND ({','.join(not_in_fields)}) NOT IN (
        SELECT {','.join(not_in_fields)} FROM {self.src_entities_alias}
    )
"""

    def _start_validity_per_seqnr(self, src_or_dst):
        """Generates the recursive WITH queries that find the begin_geldigheid for every volgnummer

        Result of this recursive query isa relation src_volgnummer_begin_geldigheid or dst_volgnummer_begin_geldigheid
        containing (_id, volgnummer, begin_geldigheid) tuples.

        """
        if src_or_dst == 'src':
            table_name = self.src_table_name
        else:
            table_name = self.dst_table_name

        return f"""
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
), {src_or_dst}_volgnummer_begin_geldigheid AS (
    SELECT
        {FIELD.ID},
        {FIELD.SEQNR},
        MIN({FIELD.START_VALIDITY}) {FIELD.START_VALIDITY}
    FROM all_{src_or_dst}_intervals
    GROUP BY {FIELD.ID}, {FIELD.SEQNR}
)"""

    def _start_validities(self):
        """Adds recursive queries to determine the begin_geldigheid for each volgnummer

        """
        result = []
        if self.src_has_states:
            result.append(self._start_validity_per_seqnr('src'))

        if self.dst_has_states:
            result.append(self._start_validity_per_seqnr('dst'))

        return result

    def _with_src_entities(self):
        return f"""
{self.src_entities_alias} AS (
    SELECT * FROM {self.src_table_name} WHERE {FIELD.LAST_EVENT} > (
        SELECT COALESCE(MAX({FIELD.LAST_SRC_EVENT}), 0) FROM {self.relation_table}
    )
)
"""

    def _with_dst_entities(self):
        return f"""
{self.dst_entities_alias} AS (
    SELECT * FROM {self.dst_table_name} WHERE {FIELD.LAST_EVENT} > (
        SELECT COALESCE(MAX({FIELD.LAST_DST_EVENT}), 0) FROM {self.relation_table}
    )
)
"""

    def _with_max_src_event(self):
        return f"""
max_src_event AS (SELECT MAX({FIELD.LAST_EVENT}) {FIELD.LAST_EVENT} FROM {self.src_table_name})
"""

    def _with_max_dst_event(self):
        return f"""
max_dst_event AS (SELECT MAX({FIELD.LAST_EVENT}) {FIELD.LAST_EVENT} FROM {self.dst_table_name})
"""

    def _with_queries(self):
        start_validities = self._start_validities()
        other_withs = [
            self._with_src_entities(),
            self._with_dst_entities(),
            self._with_max_src_event(),
            self._with_max_dst_event()
        ]

        return f"WITH{' RECURSIVE' if start_validities else ''} " \
               f"{','.join(start_validities + other_withs)}"

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

    def _join_rel(self):
        if self.src_has_states:
            return f"""
FULL JOIN (
    SELECT * FROM {self.relation_table}
    WHERE (src_id, src_volgnummer) IN (SELECT {FIELD.ID}, {FIELD.SEQNR} FROM {self.src_entities_alias})
    AND {FIELD.DATE_DELETED} IS NULL
) rel ON rel.src_id = src.{FIELD.ID} AND rel.src_volgnummer = src.{FIELD.SEQNR}
    AND {self._source_value_ref()} = rel.{FIELD.SOURCE_VALUE}
"""
        else:
            return f"""
FULL JOIN (
    SELECT * FROM {self.relation_table}
    WHERE src_id IN (SELECT {FIELD.ID} FROM {self.src_entities_alias})
    AND {FIELD.DATE_DELETED} IS NULL
) rel ON rel.src_id = src.{FIELD.ID} AND {self._source_value_ref()} = rel.{FIELD.SOURCE_VALUE}
"""

    def _get_query(self, initial_load=False):
        """Builds and returns the event extraction query

        Omits right-hand side of the query when initial_load=True, because it is unnecessary, and excluding everything
        on the right-hand side that is done on the left-hand-side creates a heavy query.
        :return:
        """

        dst_table_outer_join_on = self._dst_table_outer_join_on()

        joins = f"""
{self._join_array_elements() if self.is_many else ""}
{self._src_dst_join()}
LEFT JOIN {self.dst_table_name} dst
    ON {self.and_join.join(dst_table_outer_join_on)}
{self._join_rel()}
{self._join_src_geldigheid()}
{self._join_dst_geldigheid()}
{self._join_max_event_ids()}
"""

        query = f"""
{self._with_queries()}
SELECT
    {self.comma_join.join(self._select_expressions_src())}
FROM {self.src_entities_alias} src
{joins}
"""
        if not initial_load:
            query += f"""
UNION ALL
SELECT
    {self.comma_join.join(self._select_aliases())}
FROM (
SELECT
    {self.comma_join.join(self._select_expressions_dst())},
    {self._row_number_partition()} AS row_number
FROM {self.dst_entities_alias} dst
INNER JOIN ({self._select_rest_src()}) src ON {self.and_join.join(self._src_dst_match(f'src.{FIELD.SOURCE_VALUE}'))}
INNER JOIN {self.relation_table} rel
    ON rel.src_id=src.{FIELD.ID} {f'AND rel.src_volgnummer = src.{FIELD.SEQNR}' if self.src_has_states else ''}
    AND rel.src_source = src.{FIELD.SOURCE} AND rel.{FIELD.SOURCE_VALUE} = src.{FIELD.SOURCE_VALUE}
{self._join_src_geldigheid()}
{self._join_dst_geldigheid()}
{self._join_max_event_ids()}
) q
WHERE row_number = 1
"""
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

        if row['rel_id'] is None:
            # No relation yet, create ADD
            ignore_fields = [
                'src_deleted',
                'src_last_event',
                'rel_id',
                f'rel_{FIELD.HASH}',
                FIELD.LAST_EVENT,
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

        return relation

    def _is_initial_load(self):
        query = f"SELECT {FIELD.ID} FROM {self.relation_table} LIMIT 1"
        result = _execute(query)

        try:
            next(result)
        except StopIteration:
            return True
        return False

    def update(self):
        initial_load = self._is_initial_load()
        query = self._get_query(initial_load)
        result = _execute(query, stream=True)

        with ProgressTicker("Process relate src result", 10000) as progress, \
                ContentsWriter() as contents_writer, \
                ContentsWriter() as confirms_writer, \
                EventCollector(contents_writer, confirms_writer) as event_collector:

            filename = contents_writer.filename
            confirms = confirms_writer.filename

            for row in result:
                progress.tick()
                event = self._create_event(self._format_relation(dict(row)))
                event_collector.collect(event)

            return filename, confirms
