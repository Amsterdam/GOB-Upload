"""add brk2 aardzakelijkerechten

Revision ID: 19f159c5e167
Revises: 7c4e8e803d5b
Create Date: 2023-07-12 15:04:08.604118

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '19f159c5e167'
down_revision = '7c4e8e803d5b'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('brk2_aardzakelijkerechten',
    sa.Column('code', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('waarde', sa.String(), autoincrement=False, nullable=True),
    sa.Column('datum_vanaf', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('datum_tot', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('toelichting', sa.String(), autoincrement=False, nullable=True),
    sa.Column('akr_code', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_application', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_last_event', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('_hash', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_version', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_date_created', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_confirmed', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_modified', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_deleted', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_gobid', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_tid', sa.String(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_id', name='brk2_aardzakelijkerechten__id_key'),
    sa.UniqueConstraint('_tid', name='brk2_aardzakelijkerechten__tid_key')
    )
    op.create_table('qa_brk2_aardzakelijkerechten',
    sa.Column('meldingnummer', sa.String(), autoincrement=False, nullable=True),
    sa.Column('code', sa.String(), autoincrement=False, nullable=True),
    sa.Column('proces', sa.String(), autoincrement=False, nullable=True),
    sa.Column('attribuut', sa.String(), autoincrement=False, nullable=True),
    sa.Column('identificatie', sa.String(), autoincrement=False, nullable=True),
    sa.Column('volgnummer', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('begin_geldigheid', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('eind_geldigheid', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('betwijfelde_waarde', sa.String(), autoincrement=False, nullable=True),
    sa.Column('onderbouwing', sa.String(), autoincrement=False, nullable=True),
    sa.Column('voorgestelde_waarde', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_application', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_last_event', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('_hash', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_version', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_date_created', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_confirmed', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_modified', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_deleted', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_gobid', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_tid', sa.String(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_id', name='qa_brk2_aardzakelijkerechten__id_key'),
    sa.UniqueConstraint('_tid', name='qa_brk2_aardzakelijkerechten__tid_key')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('qa_brk2_aardzakelijkerechten')
    op.drop_table('brk2_aardzakelijkerechten')
    # ### end Alembic commands ###