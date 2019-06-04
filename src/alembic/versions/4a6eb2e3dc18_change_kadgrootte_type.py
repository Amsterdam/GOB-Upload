"""change kadgrootte type

Revision ID: 4a6eb2e3dc18
Revises: cef2c806af49
Create Date: 2019-06-04 14:57:52.691676

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '4a6eb2e3dc18'
down_revision = 'cef2c806af49'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('brk_kadastraleobjecten', 'grootte', type_=sa.DECIMAL())


def downgrade():
    op.alter_column('brk_kadastraleobjecten', 'grootte', type_=sa.Integer())
