"""Clean relation tables and events

Revision ID: 02e4df70d575
Revises: 86cb00961506
Create Date: 2020-02-17 08:55:09.351055

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '02e4df70d575'
down_revision = '86cb00961506'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("""
CREATE OR REPLACE FUNCTION clear_relations() RETURNS VOID
	LANGUAGE plpgsql
AS
$$
DECLARE
    c_tables CURSOR FOR
    SELECT * FROM pg_tables WHERE tablename LIKE 'rel_%' AND schemaname='public';
BEGIN
    FOR t in c_tables LOOP
        EXECUTE 'TRUNCATE ' || t.tablename;
    END LOOP;
    DELETE FROM events WHERE catalogue='rel';
END;
$$;
SELECT clear_relations();
""")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
