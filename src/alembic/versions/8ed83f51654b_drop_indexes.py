"""Drop indexes

Revision ID: 8ed83f51654b
Revises: 5cf4c61fcee7
Create Date: 2019-05-23 12:57:07.136503

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '8ed83f51654b'
down_revision = '5cf4c61fcee7'
branch_labels = None
depends_on = None


def upgrade():
    # Drop views to allow columns to be changed, GOB-Upload will recreate them
    op.execute('DROP INDEX IF EXISTS "events.idx.eventid"')
    op.execute('ALTER TABLE events DROP CONSTRAINT IF EXISTS events_pkey')
    op.execute('DROP INDEX IF EXISTS events_pkey')

def downgrade():
    op.execute('CREATE INDEX IF NOT EXISTS "events.idx.eventid" ON public.events USING btree (eventid)')
    op.execute('ALTER TABLE events ADD CONSTRAINT events_pkey PRIMARY KEY (eventid)')
