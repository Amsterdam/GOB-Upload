"""Add Amsterdamse sleutel

Revision ID: 99e27f51c1fe
Revises: f7967b514a38
Create Date: 2019-10-23 12:07:48.876807

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '99e27f51c1fe'
down_revision = 'f7967b514a38'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('bag_ligplaatsen', sa.Column('amsterdamse_sleutel', sa.String(), autoincrement=False, nullable=True))
    op.add_column('bag_nummeraanduidingen', sa.Column('amsterdamse_sleutel', sa.String(), autoincrement=False, nullable=True))
    op.add_column('bag_openbareruimtes', sa.Column('amsterdamse_sleutel', sa.String(), autoincrement=False, nullable=True))
    op.add_column('bag_openbareruimtes', sa.Column('straatcode', sa.String(), autoincrement=False, nullable=True))
    op.add_column('bag_panden', sa.Column('amsterdamse_sleutel', sa.String(), autoincrement=False, nullable=True))
    op.add_column('bag_standplaatsen', sa.Column('amsterdamse_sleutel', sa.String(), autoincrement=False, nullable=True))
    op.add_column('bag_verblijfsobjecten', sa.Column('amsterdamse_sleutel', sa.String(), autoincrement=False, nullable=True))
    op.add_column('bag_woonplaatsen', sa.Column('amsterdamse_sleutel', sa.String(), autoincrement=False, nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('bag_woonplaatsen', 'amsterdamse_sleutel')
    op.drop_column('bag_verblijfsobjecten', 'amsterdamse_sleutel')
    op.drop_column('bag_standplaatsen', 'amsterdamse_sleutel')
    op.drop_column('bag_panden', 'amsterdamse_sleutel')
    op.drop_column('bag_openbareruimtes', 'straatcode')
    op.drop_column('bag_openbareruimtes', 'amsterdamse_sleutel')
    op.drop_column('bag_nummeraanduidingen', 'amsterdamse_sleutel')
    op.drop_column('bag_ligplaatsen', 'amsterdamse_sleutel')
    # ### end Alembic commands ###