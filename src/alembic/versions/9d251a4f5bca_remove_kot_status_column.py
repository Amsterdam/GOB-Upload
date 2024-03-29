"""Remove KOT status column

Revision ID: 9d251a4f5bca
Revises: 888a62db7851
Create Date: 2023-03-02 06:29:47.408823

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '9d251a4f5bca'
down_revision = '888a62db7851'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("DROP VIEW IF EXISTS legacy.brk2_kadastraleobjecten")
    op.drop_column('brk2_kadastraleobjecten', 'status')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('brk2_kadastraleobjecten', sa.Column('status', sa.VARCHAR(), autoincrement=False, nullable=True))
    # ### end Alembic commands ###
