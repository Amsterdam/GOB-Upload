"""Change Neuron ID types to String

Revision ID: 5cf4c61fcee7
Revises: aaf94b61c4da
Create Date: 2019-05-22 11:23:35.561313

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '5cf4c61fcee7'
down_revision = 'aaf94b61c4da'
branch_labels = None
depends_on = None

tables = [
    'brk_aantekeningenkadastraleobjecten',
    'brk_aantekeningenrechten',
    'brk_kadastraleobjecten',
    'brk_kadastralesubjecten',
    'brk_stukdelen',
    'brk_tenaamstellingen',
    'brk_zakelijkerechten',
]


def upgrade():
    for table in tables:
        op.alter_column(table, 'id', type_=sa.String())


def downgrade():
    for table in tables:
        op.alter_column(table, 'id', type_=sa.Integer())


