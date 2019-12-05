"""Add begin eindgeldigheid for missing table

Revision ID: e70cf26a47a8
Revises: 04d8f42a1d5c
Create Date: 2019-12-05 16:00:11.611446

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = 'e70cf26a47a8'
down_revision = '04d8f42a1d5c'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('rel_brk_zrt_brk_sjt_betrokken_bij_appartementsrechtsplitsing_vv', sa.Column('begin_geldigheid', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_brk_zrt_brk_sjt_betrokken_bij_appartementsrechtsplitsing_vv', sa.Column('eind_geldigheid', sa.DateTime(), autoincrement=False, nullable=True))


def downgrade():
    op.drop_column('rel_brk_zrt_brk_sjt_betrokken_bij_appartementsrechtsplitsing_vv', 'eind_geldigheid')
    op.drop_column('rel_brk_zrt_brk_sjt_betrokken_bij_appartementsrechtsplitsing_vv', 'begin_geldigheid')
