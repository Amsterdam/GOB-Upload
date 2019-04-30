"""update volgnummer zrt and kot

Revision ID: b9a82ff463b7
Revises: e17e83508e18
Create Date: 2019-04-30 09:14:01.484663

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'b9a82ff463b7'
down_revision = 'e17e83508e18'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('brk_zakelijkerechten', 'volgnummer', type_=sa.String())
    op.alter_column('brk_kadastraleobjecten', 'volgnummer', type_=sa.String())
    op.add_column('brk_zakelijkerechten', sa.Column('_nrn_id', sa.String(), autoincrement=False, nullable=True))


def downgrade():
    op.alter_column('brk_zakelijkerechten', 'volgnummer', type_=sa.Integer())
    op.alter_column('brk_kadastraleobjecten', 'volgnummer', type_=sa.Integer())
    op.drop_column('brk_zakelijkerechten', '_nrn_id')
