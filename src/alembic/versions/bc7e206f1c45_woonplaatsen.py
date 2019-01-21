"""woonplaatsen

Revision ID: bc7e206f1c45
Revises: 065a46a6e8eb
Create Date: 2019-01-21 14:44:35.241173

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'bc7e206f1c45'
down_revision = '065a46a6e8eb'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('bag_woonplaatsen',
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
    sa.Column('naam', sa.String(), autoincrement=False, nullable=True),
    sa.Column('geometrie', geoalchemy2.types.Geometry(srid=28992), nullable=True),
    sa.Column('geconstateerd', sa.String(), autoincrement=False, nullable=True),
    sa.Column('datum_begin_geldigheid', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('datum_einde_geldigheid', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('aanduiding_in_onderzoek', sa.Boolean(), autoincrement=False, nullable=True),
    sa.Column('documentdatum', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('documentnummer', sa.String(), autoincrement=False, nullable=True),
    sa.Column('status', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('ligt_in_gemeente', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('bagproces', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('_gobid')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('bag_woonplaatsen')
    # ### end Alembic commands ###
