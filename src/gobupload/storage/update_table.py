"""Contains two classes that together update the relation table for a particular relation.

Classes are ready to be modified to use events in the future.
"""

from gobcore.events.import_events import ADD, CONFIRM, DELETE, MODIFY

from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobcore.model.relations import get_relation_name
from gobcore.sources import GOBSources

from gobupload.relate.exceptions import RelateException
from gobupload.storage.execute import _execute
from gobupload.storage.handler import GOBStorageHandler
from gobupload.storage.materialized_views import MaterializedViews

EQUALS = 'equals'
LIES_IN = 'lies_in'


class RelationTableEventExtractor:
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
        select_expressions = [
            f"src.{FIELD.ID} AS src__id",
            f"src.{FIELD.EXPIRATION_DATE} AS src__expiration_date",
            f"{self._source_value_ref()} AS src_bronwaarde",
            f"src.{FIELD.SOURCE} AS src__source",
            f"src.{FIELD.APPLICATION} AS src__application",
            f"src.{FIELD.SOURCE_ID} AS src__source_id",
            f"src.{FIELD.VERSION} AS src__version",
            f"rel.{FIELD.GOBID} AS rel__gobid",
            f"rel.src_id AS rel_src_id",
            f"rel.src_volgnummer AS rel_src_volgnummer",
            f"rel.dst_id AS rel_dst_id",
            f"rel.dst_volgnummer AS rel_dst_volgnummer",
            f"rel.{FIELD.EXPIRATION_DATE} AS rel__expiration_date",
            f"rel.{FIELD.ID} AS rel__id",
            f"CASE WHEN rel.{FIELD.VERSION} IS NOT NULL THEN rel.{FIELD.VERSION} ELSE '{self.TABLE_VERSION}' "
            f"END AS rel__version",
            f"dst.{FIELD.ID} AS dst__id",
            f"dst.{FIELD.EXPIRATION_DATE} AS dst__expiration_date",
            f"LEAST(src.{FIELD.EXPIRATION_DATE}, dst.{FIELD.EXPIRATION_DATE}) AS expected_expiration_date"
        ]

        start_validity, end_validity = self._validity_select_expressions()
        start_validity = f"CASE WHEN dst.{FIELD.ID} IS NULL THEN NULL ELSE {start_validity} END"
        end_validity = f"CASE WHEN dst.{FIELD.ID} IS NULL THEN NULL ELSE {end_validity} END"

        select_expressions += [
            f"{start_validity} AS expected_begin_geldigheid",
            f"{end_validity} AS expected_eind_geldigheid",
        ]

        if self.src_has_states:
            select_expressions.append(
                f"src.{FIELD.SEQNR} AS src_volgnummer"
            )
        if self.dst_has_states:
            select_expressions.append(
                f"dst.{FIELD.SEQNR} AS dst_volgnummer"
            )

        distinct_seqnr = f'OR dst.{FIELD.SEQNR} IS DISTINCT FROM rel.dst_volgnummer'

        select_expressions.append(f"""
    CASE
        WHEN rel.src_id IS NULL THEN '{ADD.name}'
        WHEN src.{FIELD.ID} IS NULL THEN '{DELETE.name}'
        WHEN dst.{FIELD.ID} IS DISTINCT FROM rel.dst_id
             {distinct_seqnr if self.dst_has_states else ''}
             OR LEAST(src.{FIELD.EXPIRATION_DATE}, dst.{FIELD.EXPIRATION_DATE})
             IS DISTINCT FROM rel.{FIELD.EXPIRATION_DATE}
             OR ({start_validity})::timestamp without time zone IS DISTINCT FROM rel.{FIELD.START_VALIDITY}
             OR ({end_validity})::timestamp without time zone IS DISTINCT FROM rel.{FIELD.END_VALIDITY}
             THEN '{MODIFY.name}'
        ELSE '{CONFIRM.name}'
    END AS event_type""")
        return select_expressions

    def _source_value_ref(self):
        """Returns the reference to the source value in the src object. For a many relation this reference points to
        the unpacked JSONB object.

        :return:
        """

        if self.is_many:
            return f"{self.json_join_alias}.item->>'{FIELD.SOURCE_VALUE}'"
        else:
            return f"src.{self.src_field_name}->>'{FIELD.SOURCE_VALUE}'"

    def _rel_table_join_on(self):
        """Returns the ON clause for the relation table join

        :return:
        """
        join_on = [f"src.{FIELD.ID} = rel.src{FIELD.ID}"]
        if self.src_has_states:
            join_on.append(f"src.{FIELD.SEQNR} = rel.src_{FIELD.SEQNR}")

        join_on += [
            f'rel.src_source = src.{FIELD.SOURCE}',
            f'rel.{FIELD.DATE_DELETED} IS NULL',
            f'rel.bronwaarde = {self._source_value_ref()}',
            f'rel.{FIELD.APPLICATION} = src.{FIELD.APPLICATION}',
        ]
        return join_on

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

    def _where(self):
        """WHERE clause filters all rows where src has an empty bronwaarde and there are no matching relations.
        In that case we can safely ignore this row.

        :return:
        """

        return f"WHERE NOT (" \
            f"src.{FIELD.ID} IS NOT NULL AND " \
            f"{self._source_value_ref()} IS NULL AND " \
            f"rel.{FIELD.ID} IS NULL" \
            f") AND " \
            f"rel.{FIELD.DATE_DELETED} IS NULL"

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
        AND {src_or_dst}.{FIELD.SEQNR}::int = intv.{FIELD.SEQNR}::int + 1
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

        rel_table_join_on = self._rel_table_join_on()
        dst_table_outer_join_on = self._dst_table_outer_join_on()
        select_expressions = self._select_expressions()

        joins = f"""
{self._join_array_elements() if self.is_many else ""}
FULL JOIN {self.relation_table} rel
    ON {self.and_join.join(rel_table_join_on)}
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
FROM (
    SELECT * FROM {self.src_table_name} WHERE {FIELD.DATE_DELETED} IS NULL
) src
{joins}
{self._where()}
"""

    def extract(self):
        """Extracts/generates events from the current state of the database.

        The current results are close to GOB events, but the real implementation of GOB events should be done later on.

        :return:
        """
        query = self._get_query()

        # For now (until real events are implemented in relation tables), ignore CONFIRM events
        query = f"SELECT * FROM ({query}) q WHERE event_type <> 'CONFIRM'"

        return _execute(query, stream=True)


class RelationTableUpdater:
    """RelationTableUpder updates the relation table based on the observed relations in the model.

    This class is written with the future use of real GOB events in mind.

    Usage:

    updater = RelationTableUpdater(catalog, collection, relation_field_name)
    update_cnt = updater.update_relation()

    """
    MAX_QUEUE_LENGTH = 1000

    cast_values = {
        FIELD.EXPIRATION_DATE: 'timestamp without time zone',
        FIELD.START_VALIDITY: 'timestamp without time zone',
        FIELD.END_VALIDITY: 'timestamp without time zone',
    }

    def __init__(self, src_catalog_name, src_collection_name, src_field_name):
        self.src_catalog_name = src_catalog_name
        self.src_collection_name = src_collection_name
        self.src_field_name = src_field_name
        self.events_extractor = RelationTableEventExtractor(self.src_catalog_name, self.src_collection_name,
                                                            self.src_field_name)
        self.relation_table = self.events_extractor.relation_table
        self.fields = self._get_fields()
        self.update_cnt = 0

    def _get_fields(self):
        """Returns the field mapping of database fields to the fields in the events as received from the
        EventExtractor.

        :return:
        """
        fields = {
            FIELD.GOBID: 'rel__gobid',
            FIELD.ID: None,
            FIELD.SOURCE: 'src__source',
            FIELD.APPLICATION: 'src__application',
            FIELD.SOURCE_ID: 'src__source_id',
            FIELD.VERSION: 'rel__version',
            'id': None,
            'src_source': 'src__source',
            'src_id': 'src__id',
            'dst_source': 'dst__source',
            'dst_id': 'dst__id',
            FIELD.START_VALIDITY: 'expected_begin_geldigheid',
            FIELD.END_VALIDITY: 'expected_eind_geldigheid',

            # Expected expiration date is the expiration date we expect for this relation: least(src exp, dst exp)
            # If different from real expiration_date we should trigger an update on this relation
            FIELD.EXPIRATION_DATE: 'expected_expiration_date',
            FIELD.SOURCE_VALUE: 'src_bronwaarde',
        }

        if self.events_extractor.src_has_states:
            fields['src_volgnummer'] = 'src_volgnummer'
        if self.events_extractor.dst_has_states:
            fields['dst_volgnummer'] = 'dst_volgnummer'

        return fields

    def _cast_expr(self, column_name: str):
        return f'::{self.cast_values[column_name]}' if self.cast_values.get(column_name) else ''

    def _values_list(self, event: dict, include_gob_id=False):
        """Returns the values to insert/update as a list

        :param event:
        :param include_gob_id:
        :return:
        """
        result = []
        for db_field, row_key in self.fields.items():
            if db_field == FIELD.GOBID:
                if include_gob_id:
                    # Add without quotation marks, _gobid is the only int in the row
                    result.append(str(event.get(row_key)) if event.get(row_key) else 'NULL')
                continue

            if row_key is None:
                result.append('NULL')
            else:
                result.append(f"'{event.get(row_key)}'{self._cast_expr(db_field)}" if event.get(row_key) else 'NULL')
        return result

    def _column_list(self, include_gob_id=False):
        """Returns the list of columns for an insert/update query

        :param include_gob_id:
        :return:
        """
        return ','.join([key for key in self.fields.keys() if not (key == FIELD.GOBID and include_gob_id is False)])

    def _write_events(self, queue):
        """Writes events in queue to the database. Events in queue should all be of the same type (add/modify/delete).

        :param queue:
        :return:
        """
        if not queue:
            return 0

        if queue[0]['event_type'] == ADD.name:
            self._write_add_events(queue)
        elif queue[0]['event_type'] == MODIFY.name:
            self._write_modify_events(queue)
        elif queue[0]['event_type'] == DELETE.name:
            self._write_delete_events(queue)

        self.update_cnt += len(queue)

        queue.clear()

    def _write_add_events(self, events):
        """Updates database with adds

        :param events:
        :return:
        """
        values = ",\n".join([f"({','.join(self._values_list(dict(event)))})" for event in events])
        query = f"INSERT INTO {self.relation_table} ({self._column_list()}) VALUES {values}"
        return _execute(query)

    def _write_modify_events(self, events):
        """Updates database with modifies

        :param events:
        :return:
        """
        values = ",\n".join([f"({','.join(self._values_list(dict(event), True))})" for event in events])

        set_values = ',\n'.join([f"{col_name} = v.{col_name}{self._cast_expr(col_name)}"
                                 for col_name in self.fields.keys() if col_name != FIELD.GOBID])

        query = \
            f"UPDATE {self.relation_table} AS rel\n" \
            f"SET {set_values}\n" \
            f"FROM (VALUES {values}) AS v({self._column_list(True)})\n" \
            f"WHERE rel.{FIELD.GOBID} = v.{FIELD.GOBID}"

        return _execute(query)

    def _write_delete_events(self, events):
        """Updates database with deletes

        :param events:
        :return:
        """
        ids = ','.join([str(event['rel__gobid']) for event in events])

        query = \
            f"UPDATE {self.relation_table}\n" \
            f"SET {FIELD.DATE_DELETED} = NOW()\n" \
            f"WHERE {FIELD.GOBID} IN ({ids})"

        return _execute(query)

    def _add_event_to_queue(self, event, queue):
        """Add event to the given queue. Triggers writing to the database if queue reached MAX_QUEUE_LENGTH

        :param event:
        :param queue:
        :return:
        """
        queue.append(event)

        if len(queue) >= self.MAX_QUEUE_LENGTH:
            self._write_events(queue)

    def update_relation(self):
        """Entry method.

        Gets 'events' from EventExtractor and applies these directly to the database (for now without actually creating
        any GOB events).

        :return:
        """
        events_like_objects = self.events_extractor.extract()

        # Currently not using events yet. First make sure this works in sync with the source table relations.
        # Next step will be to implement events.
        add_events = []
        modify_events = []
        delete_events = []

        for event in events_like_objects:
            if event['event_type'] == ADD.name:
                self._add_event_to_queue(event, add_events)
            elif event['event_type'] == MODIFY.name:
                self._add_event_to_queue(event, modify_events)
            elif event['event_type'] == DELETE.name:
                self._add_event_to_queue(event, delete_events)

        # Flush queues
        self._write_events(add_events)
        self._write_events(modify_events)
        self._write_events(delete_events)

        # Update materialized view
        self._refresh_materialized_view()

        return self.update_cnt

    def _refresh_materialized_view(self):
        storage_handler = GOBStorageHandler()
        materialized_views = MaterializedViews()
        mv = materialized_views.get(self.src_catalog_name, self.src_collection_name, self.src_field_name)

        mv.refresh(storage_handler)


class RelationTableChecker:
    """This class checks all relation tables against the relations defined in the source table.

    """
    model = GOBModel()

    def check_relation(self, catalog_name, collection_name, field_name):
        src_table_name = self.model.get_table_name(catalog_name, collection_name)
        relation_name = get_relation_name(self.model, catalog_name, collection_name, field_name)

        if not relation_name:
            # Destination of relation is not defined
            return []

        relation_table = "rel_" + relation_name
        src_has_states = self.model.has_states(catalog_name, collection_name)
        collection = self.model.get_collection(catalog_name, collection_name)

        src_field = collection['all_fields'].get(field_name)

        if src_field['type'] == "GOB.ManyReference":
            query = self._manyref_query(src_table_name, relation_table, field_name, src_has_states)
        elif src_field['type'] == "GOB.Reference":
            query = self._singleref_query(src_table_name, relation_table, field_name, src_has_states)
        else:
            return []

        result = _execute(query)
        src_ids = [row['src_id'] for row in result]

        return src_ids

    def _singleref_query(self, src_table_name, relation_table, field_name, src_has_states):
        src_match_seqnr = f"AND rel.src_volgnummer = src.{FIELD.SEQNR}" if src_has_states else ""

        return f"""
SELECT
    src.{FIELD.ID} as src_id
FROM {src_table_name} src
LEFT JOIN {relation_table} rel ON rel.src_id = src.{FIELD.ID} {src_match_seqnr}
AND rel.{FIELD.DATE_DELETED} IS NULL
AND rel.{FIELD.APPLICATION} = src.{FIELD.APPLICATION}
WHERE rel.dst_id <> {field_name}->>'id' OR rel.dst_volgnummer <> {field_name}->>'volgnummer'
"""

    def _manyref_query(self, src_table_name, relation_table, field_name, src_has_states):
        src_match_seqnr = f"AND rel.src_volgnummer = src.{FIELD.SEQNR}" if src_has_states else ""

        return f"""
SELECT src_id FROM (
    SELECT
        src.{FIELD.ID} as src_id,
        array_agg(distinct (ref.item->>'id', ref.item->>'volgnummer')
            order by (ref.item->>'id', ref.item->>'volgnummer')
        )::text as src_refs,
        array_agg(distinct(rel.dst_id, rel.dst_volgnummer)
            order by (rel.dst_id, rel.dst_volgnummer)
        )::text as rel_refs
    FROM {src_table_name} src
    JOIN jsonb_array_elements({field_name}) ref(item) ON TRUE
    LEFT JOIN {relation_table} rel on rel.src_id = src.{FIELD.ID} {src_match_seqnr}
    AND rel.{FIELD.DATE_DELETED} IS NULL
    AND rel.{FIELD.APPLICATION} = src.{FIELD.APPLICATION}
    GROUP BY src.{FIELD.ID}
) q
WHERE src_refs <> rel_refs
"""

    def check_collection(self, catalog_name, collection_name):
        collection = self.model.get_collection(catalog_name, collection_name)
        references = self.model._extract_references(collection['attributes'])

        for reference_name in references.keys():
            self.check_relation(catalog_name, collection_name, reference_name)

    def check_catalog(self, catalog_name):
        collections = self.model.get_collections(catalog_name)

        for collection_name in collections:
            self.check_collection(catalog_name, collection_name)

    def check_all_relations(self):
        for catalog_name in self.model.get_catalogs():
            self.check_catalog(catalog_name)
