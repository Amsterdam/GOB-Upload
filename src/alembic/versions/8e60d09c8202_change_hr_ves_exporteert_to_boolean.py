"""Change hr ves exporteert to boolean

Revision ID: 8e60d09c8202
Revises: 325814f52e9e
Create Date: 2021-03-16 05:54:25.371998

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '8e60d09c8202'
down_revision = '325814f52e9e'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('hr_vestigingen', 'exporteert', type_=sa.Boolean(), postgresql_using='exporteert::boolean')


def downgrade():
    op.alter_column('hr_vestigingen', 'exporteert', type_=sa.String())
