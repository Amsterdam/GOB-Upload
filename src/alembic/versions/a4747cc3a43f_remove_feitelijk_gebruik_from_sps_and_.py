"""Remove feitelijk_gebruik from SPS and LPS

Revision ID: a4747cc3a43f
Revises: 65ead50c5064
Create Date: 2020-10-06 13:47:28.804911

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'a4747cc3a43f'
down_revision = '65ead50c5064'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('DROP VIEW IF EXISTS "bag_ligplaatsen_enhanced_uva2"')
    op.execute('DROP VIEW IF EXISTS "bag_standplaatsen_enhanced_uva2"')

    op.drop_column('bag_ligplaatsen', 'feitelijk_gebruik')
    op.drop_column('bag_standplaatsen', 'feitelijk_gebruik')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('bag_standplaatsen', sa.Column('feitelijk_gebruik', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    op.add_column('bag_ligplaatsen', sa.Column('feitelijk_gebruik', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    # ### end Alembic commands ###
