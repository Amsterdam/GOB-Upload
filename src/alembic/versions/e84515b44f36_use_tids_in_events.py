"""use tids in events

Revision ID: e84515b44f36
Revises: f6c7514a28fe
Create Date: 2021-05-18 15:58:02.502210

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'e84515b44f36'
down_revision = 'f6c7514a28fe'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('rel_hr_ves_hr_mac_is_een_uitoefening_van', sa.Column('_tid', sa.String(), autoincrement=False, nullable=True))
    op.create_unique_constraint('rel_hr_ves_hr_mac_is_een_uitoefening_van__tid_key', 'rel_hr_ves_hr_mac_is_een_uitoefening_van', ['_tid'])

    # First generate tid's on all tables. Update events with generated tid's
    op.execute("""
create or replace function set_tids() returns void as $$
declare
    tables CURSOR FOR
    SELECT * FROM pg_tables WHERE tablename NOT LIKE 'rel_%'
                              AND tablename NOT LIKE 'test_catalogue_%'
                              AND tablename NOT IN ('events', 'spatial_ref_sys', 'alembic_version')
                              AND schemaname='public';
    res_cnt int;
    catalogue varchar;
    collection varchar;
begin
    for t in tables loop
        catalogue := split_part(t.tablename, '_', 1);

        if catalogue = 'qa' then
            collection := split_part(t.tablename, '_', 2) || '_' || split_part(t.tablename, '_', 3);
        else
            collection := split_part(t.tablename, '_', 2);
        end if;

        execute 'select count(*) from information_schema.columns where table_name=$1 and column_name=$2' into res_cnt using t.tablename, '_original_value';

        if res_cnt > 0 then
            -- This is an (old) temporary table. Ignore.
            continue;
        end if;

        -- Check if table has 'volgnummer' column
        execute 'select count(*) from information_schema.columns where table_name=$1 and column_name=$2' into res_cnt using t.tablename, 'volgnummer';

        if res_cnt > 0 and catalogue <> 'qa' then
            -- QA catalogue has volgnummer defined, but is not used. Handle as exception to this case.
            execute 'update ' || t.tablename || ' set _tid=_id || ' || quote_literal('.') || ' || volgnummer where true';
        else
            execute 'update ' || t.tablename || ' set _tid=_id where true';

        execute 'update events e
        set tid = o._tid
        from ' || t.tablename || ' o
        where o._source_id = e.source_id and o._source = e.source and
        e.catalogue=' || quote_literal(catalogue) || ' and e.entity=' || quote_literal(collection) || ' and e.tid is null';
        end if;
    end loop;
end;
$$ language plpgsql;

select set_tids();
""")

    # Update partitioning function from revision ea556acbed92: adds tid
    op.execute("""
CREATE OR REPLACE FUNCTION         insertIntoEvents (
    ev RECORD
)
RETURNS VOID
AS
$body$
DECLARE
    schema_name TEXT;
    cat_part    TEXT;
    ent_part    TEXT;
    src_part    TEXT;
BEGIN
    -- Construct catalogue and entity partition table names
    schema_name := 'events';
    -- catalogue partition: event_parts.events_catalogue
    cat_part    := schema_name || '.' || ev.catalogue;
    -- entity partition:    event_parts.events_catalogue_entity
    ent_part    := cat_part || '_' || ev.entity;
    -- source partition:    event_parts.events_catalogue_entity_source
    src_part    := ent_part || '_' || lower(ev.source);
    -- Create catalogue partition if not yet exists
    IF to_regclass(cat_part) IS NULL THEN
        -- CREATE TABLE cat_part PARTITION OF events FOR VALUES IN (catalogue) PARTITION BY LIST (entity)
        EXECUTE 'CREATE TABLE ' ||
                cat_part ||
                ' PARTITION OF events FOR VALUES IN (' ||
                quote_literal(ev.catalogue) ||
                ') PARTITION BY LIST(entity)';
    END IF;
    -- Create entity partition exists if not yet exists
    IF to_regclass(ent_part) IS NULL THEN
        -- CREATE TABLE ent_part PARTITION OF cat_part FOR VALUES IN (entity) PARTITION BY LIST (source)
        EXECUTE 'CREATE TABLE IF NOT EXISTS ' ||
                ent_part ||
                ' PARTITION OF ' || cat_part || ' FOR VALUES in (' ||
                quote_literal(ev.entity) ||
                ') PARTITION BY LIST(source)';
    END IF;
    -- Create source partition exists if not yet exists
    IF to_regclass(src_part) IS NULL THEN
        -- CREATE TABLE src_part PARTITION OF ent_part FOR VALUES IN (source)
        EXECUTE 'CREATE TABLE IF NOT EXISTS ' ||
                src_part ||
                ' PARTITION OF ' || ent_part || ' FOR VALUES in (' ||
                quote_literal(ev.source) ||
                ')';
    END IF;
    -- Insert event in the corresponding catalogue-entity-source partition
    -- INSERT INTO src_part (...) VALUES () RETURNING ()
    EXECUTE 'INSERT INTO ' ||
            src_part || ' ' ||
            '(' ||
            'eventid, ' ||
            '"timestamp", ' ||
            'catalogue, ' ||
            'entity, ' ||
            '"version", ' ||
            '"action", ' ||
            '"source", ' ||
            'source_id, ' ||
            'contents, ' ||
            'application,' ||
            'tid' ||
            ') ' ||
            'VALUES (' ||
            ev.eventid                    || ', ' ||
            quote_literal(ev.timestamp)   || ', ' ||
            quote_literal(ev.catalogue)   || ', ' ||
            quote_literal(ev.entity)      || ', ' ||
            quote_literal(ev.version)     || ', ' ||
            quote_literal(ev.action)      || ', ' ||
            quote_literal(ev.source)      || ', ' ||
            coalesce(quote_literal(ev.source_id), 'NULL') || ', ' ||
            quote_literal(ev.contents)    || ', ' ||
            quote_literal(ev.application) || ', ' ||
            quote_literal(ev.tid) ||
            ') ' ||
            'RETURNING (' ||
            ev.eventid                    || ', ' ||
            quote_literal(ev.timestamp)   || ', ' ||
            quote_literal(ev.catalogue)   || ', ' ||
            quote_literal(ev.entity)      || ', ' ||
            quote_literal(ev.version)     || ', ' ||
            quote_literal(ev.action)      || ', ' ||
            quote_literal(ev.source)      || ', ' ||
            coalesce(quote_literal(ev.source_id), 'NULL') || ', ' ||
            quote_literal(ev.contents)    || ', ' ||
            quote_literal(ev.application) || ', ' ||
            quote_literal(ev.tid) ||
            ')';
END;
$body$ LANGUAGE plpgsql;
""")

def downgrade():
    op.drop_constraint('rel_hr_ves_hr_mac_is_een_uitoefening_van__tid_key', 'rel_hr_ves_hr_mac_is_een_uitoefening_van', type_='unique')
    op.drop_column('rel_hr_ves_hr_mac_is_een_uitoefening_van', '_tid')
