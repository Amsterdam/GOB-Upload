"""events partitioning

Revision ID: ea556acbed92
Revises: d53cfff60616
Create Date: 2020-02-20 18:33:18.110196

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'ea556acbed92'
down_revision = 'd53cfff60616'
branch_labels = None
depends_on = None


def upgrade():
    # Save existing events by renaming events table
    op.execute("""
DROP  TABLE IF EXISTS        events_org CASCADE;
ALTER TABLE events RENAME TO events_org;
""")

    # Create new partitioned events table
    # Primary keys are supported on partitioned tables, foreign keys referencing partitioned tables are not supported.
    op.execute("""
CREATE TABLE public.events (
    eventid     SERIAL    NOT NULL,
    "timestamp" TIMESTAMP NOT NULL,
    catalogue   VARCHAR   NOT NULL,
    entity      VARCHAR   NOT NULL,
    "version"   VARCHAR   NOT NULL,
    "action"    VARCHAR   NOT NULL,
    "source"    VARCHAR   NOT NULL,
    source_id   VARCHAR,
    contents    JSONB     NOT NULL,
    application VARCHAR   NOT NULL,
    PRIMARY KEY (eventid, catalogue, entity, source)
) PARTITION BY LIST (catalogue);
""")

    # Create events partitions in a separate schema
    op.execute("""
DROP SCHEMA IF EXISTS events CASCADE;
CREATE SCHEMA         events;
""")

    # Create a function to automatically create partitions and insert new events
    op.execute("""
DROP FUNCTION IF EXISTS insertIntoEvents CASCADE;
CREATE FUNCTION         insertIntoEvents (
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
            'application' ||
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
            quote_literal(ev.application) ||
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
            quote_literal(ev.application) ||
            ')';
END;
$body$ LANGUAGE plpgsql;
""")

    # Call the Insert-into-events-partition function on every insert into the events table
    op.execute("""
DROP RULE IF EXISTS autoCall_insertIntoEvents ON events;
CREATE RULE         autoCall_insertIntoEvents
AS
    ON INSERT
    TO events
    DO INSTEAD (
        SELECT insertIntoEvents(NEW);
    );
""")

    # Insert all events in the new partitioned events table
    op.execute("""
INSERT INTO events SELECT * FROM events_org;
""")

    # Create indexes

    # Quickly find last eventid
    op.execute("""
DROP INDEX IF EXISTS events_eventid;
CREATE INDEX         events_eventid ON events (eventid);
""")
    # Quickly find last action for a specific entity
    op.execute("""
DROP INDEX IF EXISTS events_action;
CREATE INDEX         events_action  ON events ("action");
""")

    # -- The original events table van now be deleted
    # -- However, just for sure, leave this statement for a next migration
    # -- DROP TABLE events_org CASCADE;

    # ### end Alembic commands ###


def downgrade():
    pass
    # ### commands auto generated by Alembic - please adjust! ###
    # ### end Alembic commands ###
