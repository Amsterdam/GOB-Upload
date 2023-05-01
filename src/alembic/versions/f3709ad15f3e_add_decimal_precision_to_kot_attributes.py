"""add decimal precision to kot attributes

Revision ID: f3709ad15f3e
Revises: 36c90ad58901
Create Date: 2023-05-01 09:52:16.226329

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = 'f3709ad15f3e'
down_revision = '36c90ad58901'
branch_labels = None
depends_on = None


update_decimal_columns = (
    ('brk_kadastraleobjecten', 'perceelnummer_rotatie', 3),
    ('brk_kadastraleobjecten', 'grootte', 2),
    ('brk2_kadastraleobjecten', 'grootte', 2),
)

def upgrade():

    for tablename, column, precision in update_decimal_columns:
        op.execute(f"DROP VIEW IF EXISTS legacy.{tablename} CASCADE")
        sa_type = sa.DECIMAL(precision=10 + int(precision), scale=precision)
        op.alter_column(tablename, column, type_=sa_type)

def downgrade():
    pass
