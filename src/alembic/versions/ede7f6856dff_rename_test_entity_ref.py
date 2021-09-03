"""rename_test_entity_ref

Revision ID: ede7f6856dff
Revises: fcd6e8d1d7c3
Create Date: 2021-08-26 20:29:32.390595

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'ede7f6856dff'
down_revision = 'fcd6e8d1d7c3'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('qa_test_catalogue_test_entity_reference',
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
    sa.UniqueConstraint('_id', name='qa_test_catalogue_test_entity_reference__id_key'),
    sa.UniqueConstraint('_tid', name='qa_test_catalogue_test_entity_reference__tid_key')
    )
    op.create_table('test_catalogue_test_entity_reference',
    sa.Column('string', sa.String(), autoincrement=False, nullable=True),
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
    sa.UniqueConstraint('_id', name='test_catalogue_test_entity_reference__id_key'),
    sa.UniqueConstraint('_tid', name='test_catalogue_test_entity_reference__tid_key')
    )
    op.execute('DROP TABLE IF EXISTS qa_test_catalogue_test_entity_ref CASCADE')
    op.execute('DROP TABLE IF EXISTS test_catalogue_test_entity_ref CASCADE')
    # op.drop_constraint('rel_tst_tse_tst_ter_manyreference_dfk', 'rel_tst_tse_tst_ter_manyreference', type_='foreignkey')
    op.create_foreign_key('rel_tst_tse_tst_ter_manyreference_dfk', 'rel_tst_tse_tst_ter_manyreference', 'test_catalogue_test_entity_reference', ['dst_id'], ['_id'])
    # op.drop_constraint('rel_tst_tse_tst_ter_reference_dfk', 'rel_tst_tse_tst_ter_reference', type_='foreignkey')
    op.create_foreign_key('rel_tst_tse_tst_ter_reference_dfk', 'rel_tst_tse_tst_ter_reference', 'test_catalogue_test_entity_reference', ['dst_id'], ['_id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('rel_tst_tse_tst_ter_reference_dfk', 'rel_tst_tse_tst_ter_reference', type_='foreignkey')
    op.create_foreign_key('rel_tst_tse_tst_ter_reference_dfk', 'rel_tst_tse_tst_ter_reference', 'test_catalogue_test_entity_ref', ['dst_id'], ['_id'])
    op.drop_constraint('rel_tst_tse_tst_ter_manyreference_dfk', 'rel_tst_tse_tst_ter_manyreference', type_='foreignkey')
    op.create_foreign_key('rel_tst_tse_tst_ter_manyreference_dfk', 'rel_tst_tse_tst_ter_manyreference', 'test_catalogue_test_entity_ref', ['dst_id'], ['_id'])
    op.create_table('test_catalogue_test_entity_ref',
    sa.Column('string', sa.VARCHAR(), autoincrement=False, nullable=True),
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
    sa.PrimaryKeyConstraint('_gobid', name='test_catalogue_test_entity_ref_pkey'),
    sa.UniqueConstraint('_id', name='test_catalogue_test_entity_ref__id_key'),
    sa.UniqueConstraint('_tid', name='test_catalogue_test_entity_ref__tid_key')
    )
    op.create_table('qa_test_catalogue_test_entity_ref',
    sa.Column('meldingnummer', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('code', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('proces', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('attribuut', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('identificatie', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('volgnummer', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('betwijfelde_waarde', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('onderbouwing', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('voorgestelde_waarde', sa.VARCHAR(), autoincrement=False, nullable=True),
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
    sa.Column('begin_geldigheid', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('eind_geldigheid', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_tid', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('_gobid', name='qa_test_catalogue_test_entity_ref_pkey'),
    sa.UniqueConstraint('_id', name='qa_test_catalogue_test_entity_ref__id_key'),
    sa.UniqueConstraint('_tid', name='qa_test_catalogue_test_entity_ref__tid_key')
    )
    op.drop_table('test_catalogue_test_entity_reference')
    op.drop_table('qa_test_catalogue_test_entity_reference')
    # ### end Alembic commands ###