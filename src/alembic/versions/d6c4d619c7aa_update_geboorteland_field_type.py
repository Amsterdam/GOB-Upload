"""update geboorteland field type

Revision ID: d6c4d619c7aa
Revises: 17577285add9
Create Date: 2019-05-09 15:40:12.066476

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'd6c4d619c7aa'
down_revision = '17577285add9'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('brk_kadastralesubjecten', 'geboorteland', type_=postgresql.JSONB, postgresql_using='geboorteland::jsonb')


def downgrade():
    op.alter_column('brk_kadastralesubjecten', 'geboorteland', type_=sa.String())
