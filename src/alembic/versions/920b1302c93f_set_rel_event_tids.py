"""set rel event tids

Revision ID: 920b1302c93f
Revises: e84515b44f36
Create Date: 2021-05-20 12:04:54.117413

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '920b1302c93f'
down_revision = 'e84515b44f36'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
create or replace function set_rel_table_tids() returns void as $$
declare
    tables CURSOR FOR
    SELECT * FROM pg_tables WHERE tablename LIKE 'rel_%'
                              AND schemaname='public';
    relation varchar;
    catalogue varchar;
begin
    catalogue := 'rel';
    for t in tables loop
        relation := substring(t.tablename, 5);


        execute 'update events e
        set tid = o._tid
        from ' || t.tablename || ' o
        where o._source_id = e.source_id and o._source = e.source and
        e.catalogue=' || quote_literal(catalogue) || ' and e.entity=' || quote_literal(relation) || ' and e.tid is null';
    end loop;
end;
$$ language plpgsql;

select set_rel_table_tids();
""")


def downgrade():
    pass
