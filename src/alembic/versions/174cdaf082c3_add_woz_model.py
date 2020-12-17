"""add woz model

Revision ID: 174cdaf082c3
Revises: a4747cc3a43f
Create Date: 2020-11-10 14:54:52.557748

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '174cdaf082c3'
down_revision = 'a4747cc3a43f'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('qa_woz_wozdeelobjecten',
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
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_id', name='qa_woz_wozdeelobjecten__id_key')
    )
    op.create_table('qa_woz_wozobjecten',
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
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_id', name='qa_woz_wozobjecten__id_key')
    )
    op.create_table('woz_wozdeelobjecten',
    sa.Column('wozdeelobjectnummer', sa.String(), autoincrement=False, nullable=True),
    sa.Column('deelnummer', sa.String(), autoincrement=False, nullable=True),
    sa.Column('heeft_relatie_met_wozobject', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('soortobject', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('begin_geldigheid', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('is_verbonden_met_verblijfsobject', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('is_verbonden_met_ligplaats', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('is_verbonden_met_standplaats', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('heeft_pand', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
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
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_id', name='woz_wozdeelobjecten__id_key')
    )
    op.create_table('woz_wozobjecten',
    sa.Column('wozobjectnummer', sa.String(), autoincrement=False, nullable=True),
    sa.Column('gebruik', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('soortobject', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('begin_geldigheid', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('bevat_kadastraalobject', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
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
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_id', name='woz_wozobjecten__id_key')
    )
    op.create_table('rel_woz_wdt_bag_lps_is_verbonden_met_ligplaats',
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
    sa.ForeignKeyConstraint(['dst_id', 'dst_volgnummer'], ['bag_ligplaatsen._id', 'bag_ligplaatsen.volgnummer'], name='rel_woz_wdt_bag_lps_is_verbonden_met_ligplaats_dfk'),
    sa.ForeignKeyConstraint(['src_id'], ['woz_wozdeelobjecten._id'], name='rel_woz_wdt_bag_lps_is_verbonden_met_ligplaats_sfk'),
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_source_id', name='rel_woz_wdt_bag_lps_is_verbonden_met_ligplaats_uniq')
    )
    op.create_table('rel_woz_wdt_bag_pnd_heeft_pand',
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
    sa.ForeignKeyConstraint(['dst_id', 'dst_volgnummer'], ['bag_panden._id', 'bag_panden.volgnummer'], name='rel_woz_wdt_bag_pnd_heeft_pand_dfk'),
    sa.ForeignKeyConstraint(['src_id'], ['woz_wozdeelobjecten._id'], name='rel_woz_wdt_bag_pnd_heeft_pand_sfk'),
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_source_id', name='rel_woz_wdt_bag_pnd_heeft_pand_uniq')
    )
    op.create_table('rel_woz_wdt_bag_sps_is_verbonden_met_standplaats',
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
    sa.ForeignKeyConstraint(['dst_id', 'dst_volgnummer'], ['bag_standplaatsen._id', 'bag_standplaatsen.volgnummer'], name='rel_woz_wdt_bag_sps_is_verbonden_met_standplaats_dfk'),
    sa.ForeignKeyConstraint(['src_id'], ['woz_wozdeelobjecten._id'], name='rel_woz_wdt_bag_sps_is_verbonden_met_standplaats_sfk'),
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_source_id', name='rel_woz_wdt_bag_sps_is_verbonden_met_standplaats_uniq')
    )
    op.create_table('rel_woz_wdt_bag_vot_is_verbonden_met_verblijfsobject',
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
    sa.ForeignKeyConstraint(['dst_id', 'dst_volgnummer'], ['bag_verblijfsobjecten._id', 'bag_verblijfsobjecten.volgnummer'], name='rel_woz_wdt_bag_vot_is_verbonden_met_verblijfsobject_dfk'),
    sa.ForeignKeyConstraint(['src_id'], ['woz_wozdeelobjecten._id'], name='rel_woz_wdt_bag_vot_is_verbonden_met_verblijfsobject_sfk'),
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_source_id', name='rel_woz_wdt_bag_vot_is_verbonden_met_verblijfsobject_uniq')
    )
    op.create_table('rel_woz_wdt_woz_wot_heeft_relatie_met_wozobject',
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
    sa.ForeignKeyConstraint(['dst_id'], ['woz_wozobjecten._id'], name='rel_woz_wdt_woz_wot_heeft_relatie_met_wozobject_dfk'),
    sa.ForeignKeyConstraint(['src_id'], ['woz_wozdeelobjecten._id'], name='rel_woz_wdt_woz_wot_heeft_relatie_met_wozobject_sfk'),
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_source_id', name='rel_woz_wdt_woz_wot_heeft_relatie_met_wozobject_uniq')
    )
    op.create_table('rel_woz_wot_brk_kot_bevat_kadastraalobject',
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
    sa.ForeignKeyConstraint(['dst_id', 'dst_volgnummer'], ['brk_kadastraleobjecten._id', 'brk_kadastraleobjecten.volgnummer'], name='rel_woz_wot_brk_kot_bevat_kadastraalobject_dfk'),
    sa.ForeignKeyConstraint(['src_id'], ['woz_wozobjecten._id'], name='rel_woz_wot_brk_kot_bevat_kadastraalobject_sfk'),
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_source_id', name='rel_woz_wot_brk_kot_bevat_kadastraalobject_uniq')
    )

    op.execute('DROP VIEW IF EXISTS bag_verblijfsobjecten_enhanced_uva2')
    op.drop_column('bag_nummeraanduidingen', 'aanduiding_in_onderzoek')
    op.drop_column('bag_openbareruimtes', 'aanduiding_in_onderzoek')
    op.drop_column('bag_verblijfsobjecten', 'aanduiding_in_onderzoek')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('bag_verblijfsobjecten', sa.Column('aanduiding_in_onderzoek', sa.BOOLEAN(), autoincrement=False, nullable=True))
    op.add_column('bag_openbareruimtes', sa.Column('aanduiding_in_onderzoek', sa.BOOLEAN(), autoincrement=False, nullable=True))
    op.add_column('bag_nummeraanduidingen', sa.Column('aanduiding_in_onderzoek', sa.BOOLEAN(), autoincrement=False, nullable=True))
    op.drop_table('rel_woz_wot_brk_kot_bevat_kadastraalobject')
    op.drop_table('rel_woz_wdt_woz_wot_heeft_relatie_met_wozobject')
    op.drop_table('rel_woz_wdt_bag_vot_is_verbonden_met_verblijfsobject')
    op.drop_table('rel_woz_wdt_bag_sps_is_verbonden_met_standplaats')
    op.drop_table('rel_woz_wdt_bag_pnd_heeft_pand')
    op.drop_table('rel_woz_wdt_bag_lps_is_verbonden_met_ligplaats')
    op.drop_table('woz_wozobjecten')
    op.drop_table('woz_wozdeelobjecten')
    op.drop_table('qa_woz_wozobjecten')
    op.drop_table('qa_woz_wozdeelobjecten')
    # ### end Alembic commands ###