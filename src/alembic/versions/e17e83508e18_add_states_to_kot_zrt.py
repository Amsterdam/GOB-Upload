"""add states to kot zrt

Revision ID: e17e83508e18
Revises: 09d47f49b9f4
Create Date: 2019-04-29 13:46:32.011217

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = 'e17e83508e18'
down_revision = '09d47f49b9f4'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('brk_zakelijkerechten', sa.Column('begin_geldigheid', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('brk_zakelijkerechten', sa.Column('eind_geldigheid', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('brk_zakelijkerechten', sa.Column('registratiedatum', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('brk_zakelijkerechten', sa.Column('volgnummer', sa.Integer(), autoincrement=False, nullable=True))
    op.drop_column('brk_zakelijkerechten', '_nrn_id')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('brk_zakelijkerechten', sa.Column('_nrn_id', sa.INTEGER(), autoincrement=False, nullable=True))
    op.drop_column('brk_zakelijkerechten', 'volgnummer')
    op.drop_column('brk_zakelijkerechten', 'registratiedatum')
    op.drop_column('brk_zakelijkerechten', 'eind_geldigheid')
    op.drop_column('brk_zakelijkerechten', 'begin_geldigheid')
    # ### end Alembic commands ###
