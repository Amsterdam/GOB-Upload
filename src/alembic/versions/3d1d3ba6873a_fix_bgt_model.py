"""Fix bgt model

Revision ID: 3d1d3ba6873a
Revises: c67a11ee1b98
Create Date: 2019-10-03 09:44:01.831707

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '3d1d3ba6873a'
down_revision = 'c67a11ee1b98'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('bgt_onderbouw', 'volgnummer', type_=sa.String())


def downgrade():
    op.alter_column('bgt_onderbouw', 'volgnummer', type_=sa.Integer())
