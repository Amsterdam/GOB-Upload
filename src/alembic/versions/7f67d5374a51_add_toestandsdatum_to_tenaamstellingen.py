"""Add toestandsdatum to tenaamstellingen

Revision ID: 7f67d5374a51
Revises: a18d9a740c07
Create Date: 2019-10-01 08:58:16.692395

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '7f67d5374a51'
down_revision = 'a18d9a740c07'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('brk_tenaamstellingen', sa.Column('toestandsdatum', sa.DateTime(), autoincrement=False, nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('brk_tenaamstellingen', 'toestandsdatum')
    # ### end Alembic commands ###
