"""Change BRK meta id to string

Revision ID: 594d0ab552fb
Revises: 2e93c8e4db77
Create Date: 2021-07-05 12:17:11.527620

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '594d0ab552fb'
down_revision = '2e93c8e4db77'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('brk_meta', 'id', type_=sa.String())


def downgrade():
    op.alter_column('brk_meta', 'id', type_=sa.Integer())
