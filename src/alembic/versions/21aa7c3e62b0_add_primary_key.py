"""Add primary key

Revision ID: 21aa7c3e62b0
Revises: 8ed83f51654b
Create Date: 2019-05-23 14:23:24.500330

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '21aa7c3e62b0'
down_revision = '8ed83f51654b'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('ALTER TABLE events ADD CONSTRAINT events_pkey PRIMARY KEY (eventid)')


def downgrade():
    op.execute('ALTER TABLE events DROP CONSTRAINT IF EXISTS events_pkey')
