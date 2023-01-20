"""brk2 tng sjt relations and integer neuron_id

Revision ID: 448cdb9073bf
Revises: a58f2e8ccea5
Create Date: 2023-01-20 12:05:40.974193

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '448cdb9073bf'
down_revision = 'a58f2e8ccea5'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('rel_brk2_tng_brk2_sjt__betr_gorzen_aanwassen_brk_sjt_',
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
    sa.ForeignKeyConstraint(['dst_id'], ['brk2_kadastralesubjecten._id'], name='rel_brk2_tng_brk2_sjt__betr_gorzen_aanwassen_brk_sjt__dfk'),
    sa.ForeignKeyConstraint(['src_id'], ['brk2_tenaamstellingen._id'], name='rel_brk2_tng_brk2_sjt__betr_gorzen_aanwassen_brk_sjt__sfk'),
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_source_id', name='rel_brk2_tng_brk2_sjt__betr_gorzen_aanwassen_brk_sjt__uniq'),
    sa.UniqueConstraint('_tid', name='rel_brk2_tng_brk2_sjt__betr_gorzen_aanwassen_brk_sjt___tid_key')
    )
    op.create_table('rel_brk2_tng_brk2_sjt__betr_samenwerkverband_brk_sjt_',
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
    sa.ForeignKeyConstraint(['dst_id'], ['brk2_kadastralesubjecten._id'], name='rel_brk2_tng_brk2_sjt__betr_samenwerkverband_brk_sjt__dfk'),
    sa.ForeignKeyConstraint(['src_id'], ['brk2_tenaamstellingen._id'], name='rel_brk2_tng_brk2_sjt__betr_samenwerkverband_brk_sjt__sfk'),
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_source_id', name='rel_brk2_tng_brk2_sjt__betr_samenwerkverband_brk_sjt__uniq'),
    sa.UniqueConstraint('_tid', name='rel_brk2_tng_brk2_sjt__betr_samenwerkverband_brk_sjt___tid_key')
    )
    op.create_table('rel_brk2_tng_brk2_sjt_betrokken_partner_brk_subject',
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
    sa.ForeignKeyConstraint(['dst_id'], ['brk2_kadastralesubjecten._id'], name='rel_brk2_tng_brk2_sjt_betrokken_partner_brk_subject_dfk'),
    sa.ForeignKeyConstraint(['src_id'], ['brk2_tenaamstellingen._id'], name='rel_brk2_tng_brk2_sjt_betrokken_partner_brk_subject_sfk'),
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_source_id', name='rel_brk2_tng_brk2_sjt_betrokken_partner_brk_subject_uniq'),
    sa.UniqueConstraint('_tid', name='rel_brk2_tng_brk2_sjt_betrokken_partner_brk_subject__tid_key')
    )
    op.create_table('rel_brk2_tng_brk2_sjt_van_brk_kadastraalsubject',
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
    sa.ForeignKeyConstraint(['dst_id'], ['brk2_kadastralesubjecten._id'], name='rel_brk2_tng_brk2_sjt_van_brk_kadastraalsubject_dfk'),
    sa.ForeignKeyConstraint(['src_id'], ['brk2_tenaamstellingen._id'], name='rel_brk2_tng_brk2_sjt_van_brk_kadastraalsubject_sfk'),
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_source_id', name='rel_brk2_tng_brk2_sjt_van_brk_kadastraalsubject_uniq'),
    sa.UniqueConstraint('_tid', name='rel_brk2_tng_brk2_sjt_van_brk_kadastraalsubject__tid_key')
    )
    op.alter_column('brk2_tenaamstellingen', 'neuron_id',
               existing_type=sa.NUMERIC(),
               type_=sa.Integer(),
               existing_nullable=True,
               autoincrement=False)
    op.alter_column('brk2_tenaamstellingen', 'van_brk_kadastraalsubject',
               existing_type=sa.VARCHAR(),
               type_=postgresql.JSONB(astext_type=sa.Text()),
               existing_nullable=True,
               autoincrement=False,
               postgresql_using='van_brk_kadastraalsubject::jsonb')
    op.alter_column('brk2_tenaamstellingen', 'betrokken_partner_brk_subject',
               existing_type=sa.VARCHAR(),
               type_=postgresql.JSONB(astext_type=sa.Text()),
               existing_nullable=True,
               autoincrement=False,
               postgresql_using='betrokken_partner_brk_subject::jsonb')
    op.alter_column('brk2_tenaamstellingen', 'betrokken_samenwerkingsverband_brk_subject',
               existing_type=sa.VARCHAR(),
               type_=postgresql.JSONB(astext_type=sa.Text()),
               existing_nullable=True,
               autoincrement=False,
               postgresql_using='betrokken_samenwerkingsverband_brk_subject::jsonb')
    op.alter_column('brk2_tenaamstellingen', 'betrokken_gorzen_en_aanwassen_brk_subject',
               existing_type=sa.VARCHAR(),
               type_=postgresql.JSONB(astext_type=sa.Text()),
               existing_nullable=True,
               autoincrement=False,
               postgresql_using='betrokken_gorzen_en_aanwassen_brk_subject::jsonb')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('brk2_tenaamstellingen', 'betrokken_gorzen_en_aanwassen_brk_subject',
               existing_type=postgresql.JSONB(astext_type=sa.Text()),
               type_=sa.VARCHAR(),
               existing_nullable=True,
               autoincrement=False,
               postgresql_using='betrokken_gorzen_en_aanwassen_brk_subject::text')
    op.alter_column('brk2_tenaamstellingen', 'betrokken_samenwerkingsverband_brk_subject',
               existing_type=postgresql.JSONB(astext_type=sa.Text()),
               type_=sa.VARCHAR(),
               existing_nullable=True,
               autoincrement=False,
               postgresql_using='betrokken_samenwerkingsverband_brk_subject::text')
    op.alter_column('brk2_tenaamstellingen', 'betrokken_partner_brk_subject',
               existing_type=postgresql.JSONB(astext_type=sa.Text()),
               type_=sa.VARCHAR(),
               existing_nullable=True,
               autoincrement=False,
               postgresql_using='betrokken_partner_brk_subject::text')
    op.alter_column('brk2_tenaamstellingen', 'van_brk_kadastraalsubject',
               existing_type=postgresql.JSONB(astext_type=sa.Text()),
               type_=sa.VARCHAR(),
               existing_nullable=True,
               autoincrement=False,
               postgresql_using='van_brk_kadastraalsubject::text')
    op.alter_column('brk2_tenaamstellingen', 'neuron_id',
               existing_type=sa.Integer(),
               type_=sa.NUMERIC(),
               existing_nullable=True,
               autoincrement=False)
    op.drop_table('rel_brk2_tng_brk2_sjt_van_brk_kadastraalsubject')
    op.drop_table('rel_brk2_tng_brk2_sjt_betrokken_partner_brk_subject')
    op.drop_table('rel_brk2_tng_brk2_sjt__betr_samenwerkverband_brk_sjt_')
    op.drop_table('rel_brk2_tng_brk2_sjt__betr_gorzen_aanwassen_brk_sjt_')
    # ### end Alembic commands ###
