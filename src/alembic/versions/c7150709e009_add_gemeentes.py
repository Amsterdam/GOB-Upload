"""Add gemeentes

Revision ID: c7150709e009
Revises: d99d2d36822b
Create Date: 2019-08-01 14:54:00.746225

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = 'c7150709e009'
down_revision = 'd99d2d36822b'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('brk_gemeentes',
    sa.Column('identificatie', sa.String(), autoincrement=False, nullable=True),
    sa.Column('naam', sa.String(), autoincrement=False, nullable=True),
    sa.Column('begin_geldigheid', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('eind_geldigheid', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('geometrie', geoalchemy2.types.Geometry(srid=28992), nullable=True),
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
    sa.PrimaryKeyConstraint('_gobid')
    )
    op.create_table('rel_bag_wps_brk_gme_ligt_in_gemeente',
    sa.Column('id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('src_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('src_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('src_volgnummer', sa.String(), autoincrement=False, nullable=True),
    sa.Column('bronwaarde', sa.String(), autoincrement=False, nullable=True),
    sa.Column('derivation', sa.String(), autoincrement=False, nullable=True),
    sa.Column('dst_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('dst_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('dst_volgnummer', sa.String(), autoincrement=False, nullable=True),
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
    sa.PrimaryKeyConstraint('_gobid')
    )
    op.create_table('rel_brk_kot_brk_gme_aangeduid_door_gemeente',
    sa.Column('id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('src_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('src_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('src_volgnummer', sa.String(), autoincrement=False, nullable=True),
    sa.Column('bronwaarde', sa.String(), autoincrement=False, nullable=True),
    sa.Column('derivation', sa.String(), autoincrement=False, nullable=True),
    sa.Column('dst_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('dst_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('dst_volgnummer', sa.String(), autoincrement=False, nullable=True),
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
    sa.PrimaryKeyConstraint('_gobid')
    )
    op.create_table('rel_gbd_sdl_brk_gme_ligt_in_gemeente',
    sa.Column('id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('src_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('src_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('src_volgnummer', sa.String(), autoincrement=False, nullable=True),
    sa.Column('bronwaarde', sa.String(), autoincrement=False, nullable=True),
    sa.Column('derivation', sa.String(), autoincrement=False, nullable=True),
    sa.Column('dst_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('dst_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('dst_volgnummer', sa.String(), autoincrement=False, nullable=True),
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
    sa.PrimaryKeyConstraint('_gobid')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('rel_gbd_sdl_brk_gme_ligt_in_gemeente')
    op.drop_table('rel_brk_kot_brk_gme_aangeduid_door_gemeente')
    op.drop_table('rel_bag_wps_brk_gme_ligt_in_gemeente')
    op.drop_table('brk_gemeentes')
    # ### end Alembic commands ###