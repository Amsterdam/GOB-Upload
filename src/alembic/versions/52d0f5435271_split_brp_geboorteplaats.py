"""split_brp_geboorteplaats

Revision ID: 52d0f5435271
Revises: 415c9a3c5964
Create Date: 2021-06-07 12:53:41.945536

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '52d0f5435271'
down_revision = '415c9a3c5964'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('brp_personen', sa.Column('heeft_geboorteland', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    op.add_column('brp_personen', sa.Column('heeft_geboorteplaats_buitenland', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    op.add_column('brp_personen', sa.Column('heeft_geboorteplaats_nederland', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    op.drop_column('brp_personen', 'geboorteplaats')
    op.drop_column('brp_personen', 'geboorteland')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('brp_personen', sa.Column('geboorteland', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    op.add_column('brp_personen', sa.Column('geboorteplaats', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    op.drop_column('brp_personen', 'heeft_geboorteplaats_nederland')
    op.drop_column('brp_personen', 'heeft_geboorteplaats_buitenland')
    op.drop_column('brp_personen', 'heeft_geboorteland')
    # ### end Alembic commands ###
