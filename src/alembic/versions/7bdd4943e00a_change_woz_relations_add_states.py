"""Change woz relations, add states

Revision ID: 7bdd4943e00a
Revises: 174cdaf082c3
Create Date: 2020-12-08 09:21:36.729305

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '7bdd4943e00a'
down_revision = '174cdaf082c3'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('DROP TABLE rel_woz_wdt_woz_wot_heeft_relatie_met_wozobject CASCADE')
    op.add_column('woz_wozdeelobjecten', sa.Column('registratiedatum', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('woz_wozdeelobjecten', sa.Column('volgnummer', sa.Integer(), autoincrement=False, nullable=True))
    op.add_column('woz_wozdeelobjecten', sa.Column('wozobjectnummer', sa.String(), autoincrement=False, nullable=True))
    op.add_column('woz_wozobjecten', sa.Column('bestaat_uit_wozdeelobjecten', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    op.add_column('woz_wozobjecten', sa.Column('registratiedatum', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('woz_wozobjecten', sa.Column('volgnummer', sa.Integer(), autoincrement=False, nullable=True))
    op.drop_column('woz_wozdeelobjecten', 'heeft_relatie_met_wozobject')

    drop_constraints = [
        ('woz_wozdeelobjecten', 'woz_wozdeelobjecten__id_key'),
        ('woz_wozobjecten', 'woz_wozobjecten__id_key'),
        ('rel_woz_wdt_bag_lps_is_verbonden_met_ligplaats', 'rel_woz_wdt_bag_lps_is_verbonden_met_ligplaats_sfk'),
        ('rel_woz_wdt_bag_pnd_heeft_pand', 'rel_woz_wdt_bag_pnd_heeft_pand_sfk'),
        ('rel_woz_wdt_bag_sps_is_verbonden_met_standplaats', 'rel_woz_wdt_bag_sps_is_verbonden_met_standplaats_sfk'),
        ('rel_woz_wdt_bag_vot_is_verbonden_met_verblijfsobject', 'rel_woz_wdt_bag_vot_is_verbonden_met_verblijfsobject_sfk'),
        ('rel_woz_wot_brk_kot_bevat_kadastraalobject', 'rel_woz_wot_brk_kot_bevat_kadastraalobject_sfk'),
    ]
    for table, constraint in drop_constraints:
        op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint} CASCADE")

    op.create_unique_constraint('woz_wozdeelobjecten__id_volgnummer_key', 'woz_wozdeelobjecten', ['_id', 'volgnummer'])
    op.create_unique_constraint('woz_wozobjecten__id_volgnummer_key', 'woz_wozobjecten', ['_id', 'volgnummer'])
    op.create_foreign_key('rel_woz_wdt_bag_lps_is_verbonden_met_ligplaats_sfk', 'rel_woz_wdt_bag_lps_is_verbonden_met_ligplaats', 'woz_wozdeelobjecten', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_woz_wdt_bag_pnd_heeft_pand_sfk', 'rel_woz_wdt_bag_pnd_heeft_pand', 'woz_wozdeelobjecten', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_woz_wdt_bag_sps_is_verbonden_met_standplaats_sfk', 'rel_woz_wdt_bag_sps_is_verbonden_met_standplaats', 'woz_wozdeelobjecten', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_woz_wdt_bag_vot_is_verbonden_met_verblijfsobject_sfk', 'rel_woz_wdt_bag_vot_is_verbonden_met_verblijfsobject', 'woz_wozdeelobjecten', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_woz_wot_brk_kot_bevat_kadastraalobject_sfk', 'rel_woz_wot_brk_kot_bevat_kadastraalobject', 'woz_wozobjecten', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])

    op.create_table('rel_woz_wot_woz_wdt_bestaat_uit_wozdeelobjecten',
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
    sa.ForeignKeyConstraint(['dst_id', 'dst_volgnummer'], ['woz_wozdeelobjecten._id', 'woz_wozdeelobjecten.volgnummer'], name='rel_woz_wot_woz_wdt_bestaat_uit_wozdeelobjecten_dfk'),
    sa.ForeignKeyConstraint(['src_id', 'src_volgnummer'], ['woz_wozobjecten._id', 'woz_wozobjecten.volgnummer'], name='rel_woz_wot_woz_wdt_bestaat_uit_wozdeelobjecten_sfk'),
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_source_id', name='rel_woz_wot_woz_wdt_bestaat_uit_wozdeelobjecten_uniq')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('rel_woz_wot_woz_wdt_bestaat_uit_wozdeelobjecten')
    op.create_unique_constraint('woz_wozobjecten__id_key', 'woz_wozobjecten', ['_id'])
    op.drop_constraint('woz_wozobjecten__id_volgnummer_key', 'woz_wozobjecten', type_='unique')
    op.drop_column('woz_wozobjecten', 'volgnummer')
    op.drop_column('woz_wozobjecten', 'registratiedatum')
    op.drop_column('woz_wozobjecten', 'bestaat_uit_wozdeelobjecten')
    op.add_column('woz_wozdeelobjecten', sa.Column('heeft_relatie_met_wozobject', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    op.create_unique_constraint('woz_wozdeelobjecten__id_key', 'woz_wozdeelobjecten', ['_id'])
    op.drop_constraint('woz_wozdeelobjecten__id_volgnummer_key', 'woz_wozdeelobjecten', type_='unique')
    op.drop_column('woz_wozdeelobjecten', 'wozobjectnummer')
    op.drop_column('woz_wozdeelobjecten', 'volgnummer')
    op.drop_column('woz_wozdeelobjecten', 'registratiedatum')
    op.drop_constraint('rel_woz_wot_brk_kot_bevat_kadastraalobject_sfk', 'rel_woz_wot_brk_kot_bevat_kadastraalobject', type_='foreignkey')
    op.create_foreign_key('rel_woz_wot_brk_kot_bevat_kadastraalobject_sfk', 'rel_woz_wot_brk_kot_bevat_kadastraalobject', 'woz_wozobjecten', ['src_id'], ['_id'])
    op.drop_constraint('rel_woz_wdt_bag_vot_is_verbonden_met_verblijfsobject_sfk', 'rel_woz_wdt_bag_vot_is_verbonden_met_verblijfsobject', type_='foreignkey')
    op.create_foreign_key('rel_woz_wdt_bag_vot_is_verbonden_met_verblijfsobject_sfk', 'rel_woz_wdt_bag_vot_is_verbonden_met_verblijfsobject', 'woz_wozdeelobjecten', ['src_id'], ['_id'])
    op.drop_constraint('rel_woz_wdt_bag_sps_is_verbonden_met_standplaats_sfk', 'rel_woz_wdt_bag_sps_is_verbonden_met_standplaats', type_='foreignkey')
    op.create_foreign_key('rel_woz_wdt_bag_sps_is_verbonden_met_standplaats_sfk', 'rel_woz_wdt_bag_sps_is_verbonden_met_standplaats', 'woz_wozdeelobjecten', ['src_id'], ['_id'])
    op.drop_constraint('rel_woz_wdt_bag_pnd_heeft_pand_sfk', 'rel_woz_wdt_bag_pnd_heeft_pand', type_='foreignkey')
    op.create_foreign_key('rel_woz_wdt_bag_pnd_heeft_pand_sfk', 'rel_woz_wdt_bag_pnd_heeft_pand', 'woz_wozdeelobjecten', ['src_id'], ['_id'])
    op.drop_constraint('rel_woz_wdt_bag_lps_is_verbonden_met_ligplaats_sfk', 'rel_woz_wdt_bag_lps_is_verbonden_met_ligplaats', type_='foreignkey')
    op.create_foreign_key('rel_woz_wdt_bag_lps_is_verbonden_met_ligplaats_sfk', 'rel_woz_wdt_bag_lps_is_verbonden_met_ligplaats', 'woz_wozdeelobjecten', ['src_id'], ['_id'])
    op.create_table('rel_woz_wdt_woz_wot_heeft_relatie_met_wozobject',
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
    sa.ForeignKeyConstraint(['dst_id'], ['woz_wozobjecten._id'], name='rel_woz_wdt_woz_wot_heeft_relatie_met_wozobject_dfk'),
    sa.ForeignKeyConstraint(['src_id'], ['woz_wozdeelobjecten._id'], name='rel_woz_wdt_woz_wot_heeft_relatie_met_wozobject_sfk'),
    sa.PrimaryKeyConstraint('_gobid', name='rel_woz_wdt_woz_wot_heeft_relatie_met_wozobject_pkey'),
    sa.UniqueConstraint('_source_id', name='rel_woz_wdt_woz_wot_heeft_relatie_met_wozobject_uniq')
    )
    # ### end Alembic commands ###
