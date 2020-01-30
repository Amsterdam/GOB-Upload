"""Contains two classes that together update the relation table for a particular relation.

Classes are ready to be modified to use events in the future.
"""
from datetime import date, datetime

from gobcore.message_broker.offline_contents import ContentsWriter

from gobcore.logging.logger import logger
from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobcore.model.relations import get_relation_name
from gobcore.sources import GOBSources
from gobcore.utils import ProgressTicker

from gobupload.relate.exceptions import RelateException
from gobupload.storage.execute import _execute

EQUALS = 'equals'
LIES_IN = 'lies_in'
GOB = 'GOB'


class RelationTableRelater:
    """RelationTableEventExtractor

    Generates events based on the current state of the source table and the relation table.

    Currently generates objects that resemble GOB events, but are not actually GOB events. In the future the extract()
    method should generate proper GOB events.
    """
    TABLE_VERSION = '0.1'

    model = GOBModel()
    sources = GOBSources()

    space_join = ' \n    '
    comma_join = ',\n    '
    and_join = ' AND\n    '
    or_join = ' OR\n    '

    json_join_alias = 'json_arr_elm'

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

    def _select_expressions(self):
        """Returns the select expressions for the outer query

        :return:
        """
        # example = {
        #     '_version': '',
        #     '_application': '',
        #     '_source_id': '',
        #     '_source': '',
        #     '_expiration_date': '',
        #     'id': '',                   # concat: src_id . src_volgnummer . src_source . bronwaarde
        #     'src_source': '',
        #     'src_id': '',
        #     'src_volgnummer': '',
        #     'derivation': '',
        #     'dst_source': '',
        #     'dst_id': '',
        #     'dst_volgnummer': '',
        #     'bronwaarde': '',
        #     'begin_geldigheid': '',
        #     'eind_geldigheid': '',
        # }

        select_expressions = [
            f"src.{FIELD.VERSION} AS {FIELD.VERSION}",
            f"src.{FIELD.APPLICATION} AS {FIELD.APPLICATION}",
            f"src.{FIELD.SOURCE_ID} AS {FIELD.SOURCE_ID}",
            f"'{GOB}' AS {FIELD.SOURCE}",
            f"LEAST(src.{FIELD.EXPIRATION_DATE}, dst.{FIELD.EXPIRATION_DATE}) AS {FIELD.EXPIRATION_DATE}",
            f"{self._get_id()} AS id",
            f"{self._get_derivation()} AS derivation",  # match if method == 'equals' else method
            f"src.{FIELD.SOURCE} AS src_source",
            f"src.{FIELD.ID} AS src_id",
            f"dst.{FIELD.SOURCE} AS dst_source",
            f"dst.{FIELD.ID} AS dst_id",
            f"{self._source_value_ref()} AS {FIELD.SOURCE_VALUE}",
        ]

        start_validity, end_validity = self._validity_select_expressions()
        start_validity = f"CASE WHEN dst.{FIELD.ID} IS NULL THEN NULL ELSE {start_validity} END"
        end_validity = f"CASE WHEN dst.{FIELD.ID} IS NULL THEN NULL ELSE {end_validity} END"

        select_expressions += [
            f"{start_validity} AS {FIELD.START_VALIDITY}",
            f"{end_validity} AS {FIELD.END_VALIDITY}",
        ]

        if self.src_has_states:
            select_expressions.append(
                f"src.{FIELD.SEQNR} AS src_volgnummer"
            )
        if self.dst_has_states:
            select_expressions.append(
                f"dst.{FIELD.SEQNR} AS dst_volgnummer"
            )

        return select_expressions

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

    def _geo_resolve(self, spec, src_ref='src'):
        src_geo = f"{src_ref}.{spec['source_attribute']}"
        dst_geo = f"dst.{spec['destination_attribute']}"

        resolvers = {
            LIES_IN: f"ST_IsValid({dst_geo}) "
                     f"AND ST_Contains({dst_geo}::geometry, ST_PointOnSurface({src_geo}::geometry))"
        }
        return resolvers.get(spec["method"])

    def _json_obj_ref(self, src_ref='src'):
        return f'{self.json_join_alias}.item' if self.is_many else f'{src_ref}.{self.src_field_name}'

    def _relate_match(self, spec, src_ref='src'):
        if spec['method'] == EQUALS:
            return f"dst.{spec['destination_attribute']} = {self._json_obj_ref(src_ref)}->>'bronwaarde'"
        else:
            return self._geo_resolve(spec, src_ref)

    def _dst_table_inner_join_on(self, src_ref='src'):
        """Returns the ON clause for the dst table join

        :param src_ref:
        :return:
        """
        join_on = [f"({src_ref}.{FIELD.APPLICATION} = '{spec['source']}' AND " +
                   f"{self._relate_match(spec, src_ref)})" for spec in self.relation_specs]
        # If more matches have been defined that catch any of the matches
        if len(join_on) > 1:
            join_on = [f"({self.or_join.join(join_on)})"]
        # If both collections have states then join with corresponding geldigheid intervals
        if self.src_has_states and self.dst_has_states:
            join_on.extend([
                f"(dst.{FIELD.START_VALIDITY} < {src_ref}.{FIELD.END_VALIDITY} "
                f"OR {src_ref}.{FIELD.END_VALIDITY} IS NULL)",
                f"(dst.{FIELD.END_VALIDITY} >= {src_ref}.{FIELD.END_VALIDITY} "
                f"OR dst.{FIELD.END_VALIDITY} IS NULL)"
            ])
        elif self.dst_has_states:
            # If only destination has states, get the destination that is valid until forever
            join_on.extend([
                f"(dst.{FIELD.END_VALIDITY} IS NULL OR dst.{FIELD.END_VALIDITY} > NOW())"
            ])

        join_on.append(f'dst.{FIELD.DATE_DELETED} IS NULL')
        return join_on

    def _dst_table_outer_join_on(self):
        join_on = [f"dst.{FIELD.ID} = src_dst.dst_id"]

        if self.dst_has_states:
            join_on.append(f"dst.{FIELD.SEQNR} = src_dst.dst_volgnummer")

        return join_on

    def _src_dst_join_on(self):
        join_on = [f"src_dst.src_id = src.{FIELD.ID}"]

        if self.src_has_states:
            join_on.append(f"src_dst.src_volgnummer = src.{FIELD.SEQNR}")

        join_on.append(f"src_dst.bronwaarde = {self._json_obj_ref('src')}->>'bronwaarde'")
        return join_on

    def _src_dst_select_expressions(self):
        expressions = [f"src.{FIELD.ID} AS src_id",
                       f"dst.{FIELD.ID} AS dst_id",
                       f"{self._json_obj_ref('src')}->>'bronwaarde' as bronwaarde"]

        if self.src_has_states:
            expressions.append(f"src.{FIELD.SEQNR} as src_volgnummer")

        if self.dst_has_states:
            expressions.append(f"max(dst.{FIELD.SEQNR}) as dst_volgnummer")

        return expressions

    def _src_dst_group_by(self):
        group_by = [f"src.{FIELD.ID}", f"dst.{FIELD.ID}", "bronwaarde"]

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
        return f"JOIN jsonb_array_elements(src.{self.src_field_name}) {self.json_join_alias}(item) ON TRUE"

    def _src_dst_join(self):
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
        SELECT * FROM {self.src_table_name} WHERE {FIELD.DATE_DELETED} IS NULL
    ) src
    {validgeo_src}
    {self._join_array_elements() if self.is_many else ""}
    LEFT JOIN {self.dst_table_name} dst ON {self.and_join.join(self._dst_table_inner_join_on())}
    GROUP BY {self.comma_join.join(self._src_dst_group_by())}
) src_dst ON {self.and_join.join(self._src_dst_join_on())}
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
        AND t.{FIELD.SEQNR}::int < s.{FIELD.SEQNR}::int
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
        AND {src_or_dst}.{FIELD.SEQNR}::int > intv.{FIELD.SEQNR}::int
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

        if result:
            return f"WITH RECURSIVE {','.join(result)}"
        else:
            return ""

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

    def _get_query(self):
        """Builds and returns the event extraction query

        :return:
        """

        dst_table_outer_join_on = self._dst_table_outer_join_on()
        select_expressions = self._select_expressions()

        joins = f"""
{self._join_array_elements() if self.is_many else ""}
{self._src_dst_join()}
LEFT JOIN {self.dst_table_name} dst
    ON {self.and_join.join(dst_table_outer_join_on)}
{self._join_src_geldigheid()}
{self._join_dst_geldigheid()}
"""

        return f"""
{self._start_validities()}
SELECT
    {self.comma_join.join(select_expressions)}
FROM {self.src_table_name} src
{joins}
WHERE src.{FIELD.DATE_DELETED} IS NULL
"""

    def extract_relations(self):
        """Extracts relations from the current state of the database.
        :return:
        """
        query = self._get_query()
        return _execute(query, stream=True)


class RelationTableUpdater:
    """RelationTableUpdater generates relation entities based on the observed relations in the model.

    This class imports the new relations just as all other entities; compare will be triggered, events will be
    generated, etcetera.

    The output of this class can be seen as the import step of entities from the rel catalog.

    Usage:

    updater = RelationTableUpdater(catalog, collection, relation_field_name)
    update_cnt = updater.update_relation()

    """

    def __init__(self, src_catalog_name, src_collection_name, src_field_name):
        self.src_catalog_name = src_catalog_name
        self.src_collection_name = src_collection_name
        self.src_field_name = src_field_name
        self.relater = RelationTableRelater(self.src_catalog_name, self.src_collection_name,
                                            self.src_field_name)
        self.relation_table = self.relater.relation_table
        self.filename = None

    def _format_relation(self, relation: dict):
        timestamps = [FIELD.START_VALIDITY, FIELD.END_VALIDITY, FIELD.EXPIRATION_DATE]

        for field in timestamps:
            # Add time-part to date objects
            if relation.get(field) and isinstance(relation.get(field), date):
                relation[field] = datetime.combine(relation.get(field), datetime.min.time())

        return relation

    def update_relation(self):
        """Entry method.

        :return:
        """

        relation_objects = self.relater.extract_relations()

        cnt = 0
        with ContentsWriter() as writer, \
                ProgressTicker(
                    f"Write relation {self.src_catalog_name} {self.src_collection_name} {self.src_field_name}",
                    10000) as progress:
            for relation in relation_objects:
                progress.tick()
                cnt += 1
                writer.write(self._format_relation(dict(relation)))

        logger.info(f"Written {cnt} relations")
        return writer.filename
