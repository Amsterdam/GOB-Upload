"""update zrt model for export

Revision ID: 0c0ae011ab46
Revises: 5cf4c61fcee7
Create Date: 2019-05-23 10:18:33.623393

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '0c0ae011ab46'
down_revision = '21aa7c3e62b0'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('brk_zakelijkerechten', sa.Column('betrokken_bij_appartementsrechtsplitsing', sa.String(), autoincrement=False, nullable=True))
    op.add_column('brk_zakelijkerechten', sa.Column('ontstaan_uit_appartementsrechtsplitsing', sa.String(), autoincrement=False, nullable=True))
    op.alter_column('brk_zakelijkerechten', 'appartementsrechtsplitsingidentificatie', type_=sa.String())
    op.add_column('brk_kadastraleobjecten', sa.Column('wijzigingsdatum', sa.DateTime(), autoincrement=False, nullable=True))


def downgrade():
    op.drop_column('brk_kadastraleobjecten', 'wijzigingsdatum')
    op.alter_column('brk_zakelijkerechten', 'appartementsrechtsplitsingidentificatie', type_=sa.Integer())
    op.drop_column('brk_zakelijkerechten', 'ontstaan_uit_appartementsrechtsplitsing')
    op.drop_column('brk_zakelijkerechten', 'betrokken_bij_appartementsrechtsplitsing')

