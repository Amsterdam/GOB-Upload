"""Migrate relation _id's

Revision ID: 9681961b1b9c
Revises: e1953941d667
Create Date: 2021-04-28 15:03:52.479895

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '9681961b1b9c'
down_revision = 'e1953941d667'
branch_labels = None
depends_on = None


def upgrade():
    """
    This migration sets the _id in all past relation table events, and subsequently updates the _id accordingly in the
    relation tables.

    :return:
    """
    op.execute('''
create or replace function set_rel_ids() returns void as $$
declare
    tables CURSOR FOR
    SELECT * FROM pg_tables WHERE tablename LIKE 'rel_%'
                              AND schemaname='public';
begin
    -- Update ADD events first. Add _id to events
    update events e
    set contents = e.contents #- '{entity}' || jsonb_build_object('entity', e.contents->'entity' || jsonb_build_object('_id', e.contents->'entity'->'id'))
    where e.catalogue='rel' and e.action = 'ADD' and e.contents->'entity'->'_id' is null;

    -- Now update all relation tables
    for t in tables loop
        execute 'update ' || t.tablename || ' set _id=id';
    end loop;
end;
$$ language plpgsql;

select set_rel_ids();''')


def downgrade():
    pass
