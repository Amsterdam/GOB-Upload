"""add_ligt_in_buurt_to_panden

Revision ID: 552399e55e36
Revises: 920b1302c93f
Create Date: 2021-05-20 15:29:15.897813

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '552399e55e36'
down_revision = '920b1302c93f'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('rel_bag_pnd_gbd_brt_ligt_in_buurt',
    sa.Column('id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('src_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('src_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('src_volgnummer', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('bronwaarde', sa.String(), autoincrement=False, nullable=True),
    sa.Column('derivation', sa.String(), autoincrement=False, nullable=True),
    sa.Column('dst_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('dst_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('dst_volgnummer', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('begin_geldigheid', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('eind_geldigheid', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_last_src_event', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('_last_dst_event', sa.Integer(), autoincrement=False, nullable=True),
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
    sa.ForeignKeyConstraint(['dst_id', 'dst_volgnummer'], ['gebieden_buurten._id', 'gebieden_buurten.volgnummer'], name='rel_bag_pnd_gbd_brt_ligt_in_buurt_dfk'),
    sa.ForeignKeyConstraint(['src_id', 'src_volgnummer'], ['bag_panden._id', 'bag_panden.volgnummer'], name='rel_bag_pnd_gbd_brt_ligt_in_buurt_sfk'),
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_source_id', name='rel_bag_pnd_gbd_brt_ligt_in_buurt_uniq'),
    sa.UniqueConstraint('_tid', name='rel_bag_pnd_gbd_brt_ligt_in_buurt__tid_key')
    )
    op.add_column('bag_panden', sa.Column('ligt_in_buurt', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('bag_panden', 'ligt_in_buurt')
    op.drop_table('rel_bag_pnd_gbd_brt_ligt_in_buurt')
    # ### end Alembic commands ###