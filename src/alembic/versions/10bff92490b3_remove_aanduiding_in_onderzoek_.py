"""Remove aanduiding_in_onderzoek attribute from LPS and WPS

Revision ID: 10bff92490b3
Revises: 31243f87ad52
Create Date: 2020-06-24 13:48:12.648302

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '10bff92490b3'
down_revision = '31243f87ad52'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('DROP VIEW IF EXISTS "bag_ligplaatsen_enhanced_uva2"')

    op.drop_column('bag_ligplaatsen', 'aanduiding_in_onderzoek')
    op.drop_column('bag_woonplaatsen', 'aanduiding_in_onderzoek')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('bag_woonplaatsen', sa.Column('aanduiding_in_onderzoek', sa.BOOLEAN(), autoincrement=False, nullable=True))
    op.add_column('bag_ligplaatsen', sa.Column('aanduiding_in_onderzoek', sa.BOOLEAN(), autoincrement=False, nullable=True))
    # ### end Alembic commands ###