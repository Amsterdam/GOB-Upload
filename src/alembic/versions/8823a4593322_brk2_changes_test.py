"""brk2 changes test

Revision ID: 8823a4593322
Revises: c4db521e3e93
Create Date: 2023-03-21 16:06:20.647453

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '8823a4593322'
down_revision = 'c4db521e3e93'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('DROP TABLE IF EXISTS rel_brk2_zrt_brk2_zrt__betr_apprechtsplit_vve_ CASCADE')
    op.execute('DROP VIEW IF EXISTS legacy."brk2_zakelijkerechten"')
    op.drop_column('brk2_zakelijkerechten', 'betrokken_bij_appartementsrechtsplitsing_vve')
    op.add_column('brk2_zakelijkerechten',
               sa.Column('betrokken_bij_appartementsrechtsplitsing_vve', type_=sa.String(),
               autoincrement=False, nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('DROP VIEW IF EXISTS legacy."brk2_zakelijkerechten"')
    op.alter_column('brk2_zakelijkerechten', 'betrokken_bij_appartementsrechtsplitsing_vve',
               existing_type=sa.String(),
               type_=postgresql.JSONB(astext_type=sa.Text()),
               existing_nullable=True,
               postgresql_using='to_jsonb(betrokken_bij_appartementsrechtsplitsing_vve)')
    op.create_table('rel_brk2_zrt_brk2_zrt__betr_apprechtsplit_vve_',
    sa.Column('id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('src_source', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('src_id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('src_volgnummer', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('bronwaarde', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('derivation', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('dst_source', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('dst_id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('dst_volgnummer', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('begin_geldigheid', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('eind_geldigheid', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_last_src_event', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('_last_dst_event', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('_source', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_application', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_source_id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_last_event', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('_hash', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_version', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_date_created', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_date_confirmed', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_date_modified', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_date_deleted', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_expiration_date', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_gobid', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('_id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_tid', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.ForeignKeyConstraint(['dst_id', 'dst_volgnummer'], ['brk2_zakelijkerechten._id', 'brk2_zakelijkerechten.volgnummer'], name='rel_brk2_zrt_brk2_zrt__betr_apprechtsplit_vve__dfk'),
    sa.ForeignKeyConstraint(['src_id', 'src_volgnummer'], ['brk2_zakelijkerechten._id', 'brk2_zakelijkerechten.volgnummer'], name='rel_brk2_zrt_brk2_zrt__betr_apprechtsplit_vve__sfk'),
    sa.PrimaryKeyConstraint('_gobid', name='rel_brk2_zrt_brk2_zrt__betr_apprechtsplit_vve__pkey'),
    sa.UniqueConstraint('_source_id', name='rel_brk2_zrt_brk2_zrt__betr_apprechtsplit_vve__uniq'),
    sa.UniqueConstraint('_tid', name='rel_brk2_zrt_brk2_zrt__betr_apprechtsplit_vve___tid_key')
    )
    # ### end Alembic commands ###
