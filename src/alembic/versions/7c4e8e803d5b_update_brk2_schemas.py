"""update brk2 schemas

Revision ID: 7c4e8e803d5b
Revises: 98c6dfc4986e
Create Date: 2023-07-07 07:22:43.410094

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '7c4e8e803d5b'
down_revision = '98c6dfc4986e'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("DROP VIEW IF EXISTS legacy.brk2_stukdelen")
    op.add_column('brk2_stukdelen', sa.Column('is_bron_voor_brk_erfpachtcanon', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))


def downgrade():
    op.drop_column('brk2_stukdelen', 'is_bron_voor_brk_erfpachtcanon')
