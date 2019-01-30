"""standplaatsen

Revision ID: 5233d5b47487
Revises: 476c86e0d815
Create Date: 2019-01-30 14:29:31.209169

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '5233d5b47487'
down_revision = '476c86e0d815'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('bag_standplaatsen',
    sa.Column('_gobid', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_application', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_last_event', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('_version', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_date_created', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_confirmed', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_modified', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_deleted', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('volgnummer', sa.String(), autoincrement=False, nullable=True),
    sa.Column('registratiedatum', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('identificatie', sa.String(), autoincrement=False, nullable=True),
    sa.Column('geconstateerd', sa.Boolean(), autoincrement=False, nullable=True),
    sa.Column('status', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('heeft_hoofdadres', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('heeft_nevenadres', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('geometrie', geoalchemy2.types.Geometry(srid=28992), nullable=True),
    sa.Column('begin_geldigheid', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('eind_geldigheid', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('aanduiding_in_onderzoek', sa.Boolean(), autoincrement=False, nullable=True),
    sa.Column('documentdatum', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('documentnummer', sa.String(), autoincrement=False, nullable=True),
    sa.Column('ligt_in_buurt', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('bagproces', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('_gobid')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('bag_standplaatsen')
    # ### end Alembic commands ###
