"""rel foreign keys

Revision ID: d53cfff60616
Revises: 030c8d16a3be
Create Date: 2020-02-24 11:04:23.197207

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = 'd53cfff60616'
down_revision = '030c8d16a3be'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_foreign_key('rel_bag_dsr_bag_bdt_heeft_brondocumenten_dfk', 'rel_bag_dsr_bag_bdt_heeft_brondocumenten', 'bag_brondocumenten', ['dst_id'], ['_id'])
    op.create_foreign_key('rel_bag_dsr_bag_bdt_heeft_brondocumenten_sfk', 'rel_bag_dsr_bag_bdt_heeft_brondocumenten', 'bag_dossiers', ['src_id'], ['_id'])
    op.create_foreign_key('rel_bag_lps_bag_dsr_heeft_dossier_sfk', 'rel_bag_lps_bag_dsr_heeft_dossier', 'bag_ligplaatsen', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_lps_bag_dsr_heeft_dossier_dfk', 'rel_bag_lps_bag_dsr_heeft_dossier', 'bag_dossiers', ['dst_id'], ['_id'])
    op.create_foreign_key('rel_bag_lps_bag_nag_heeft_hoofdadres_sfk', 'rel_bag_lps_bag_nag_heeft_hoofdadres', 'bag_ligplaatsen', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_lps_bag_nag_heeft_hoofdadres_dfk', 'rel_bag_lps_bag_nag_heeft_hoofdadres', 'bag_nummeraanduidingen', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_lps_bag_nag_heeft_nevenadres_sfk', 'rel_bag_lps_bag_nag_heeft_nevenadres', 'bag_ligplaatsen', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_lps_bag_nag_heeft_nevenadres_dfk', 'rel_bag_lps_bag_nag_heeft_nevenadres', 'bag_nummeraanduidingen', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_lps_gbd_brt_ligt_in_buurt_dfk', 'rel_bag_lps_gbd_brt_ligt_in_buurt', 'gebieden_buurten', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_lps_gbd_brt_ligt_in_buurt_sfk', 'rel_bag_lps_gbd_brt_ligt_in_buurt', 'bag_ligplaatsen', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_nag_bag_dsr_heeft_dossier_sfk', 'rel_bag_nag_bag_dsr_heeft_dossier', 'bag_nummeraanduidingen', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_nag_bag_dsr_heeft_dossier_dfk', 'rel_bag_nag_bag_dsr_heeft_dossier', 'bag_dossiers', ['dst_id'], ['_id'])
    op.create_foreign_key('rel_bag_nag_bag_lps_adresseert_ligplaats_sfk', 'rel_bag_nag_bag_lps_adresseert_ligplaats', 'bag_nummeraanduidingen', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_nag_bag_lps_adresseert_ligplaats_dfk', 'rel_bag_nag_bag_lps_adresseert_ligplaats', 'bag_ligplaatsen', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_nag_bag_ore_ligt_aan_openbareruimte_dfk', 'rel_bag_nag_bag_ore_ligt_aan_openbareruimte', 'bag_openbareruimtes', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_nag_bag_ore_ligt_aan_openbareruimte_sfk', 'rel_bag_nag_bag_ore_ligt_aan_openbareruimte', 'bag_nummeraanduidingen', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_nag_bag_sps_adresseert_standplaats_sfk', 'rel_bag_nag_bag_sps_adresseert_standplaats', 'bag_nummeraanduidingen', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_nag_bag_sps_adresseert_standplaats_dfk', 'rel_bag_nag_bag_sps_adresseert_standplaats', 'bag_standplaatsen', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_nag_bag_vot_adresseert_verblijfsobject_dfk', 'rel_bag_nag_bag_vot_adresseert_verblijfsobject', 'bag_verblijfsobjecten', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_nag_bag_vot_adresseert_verblijfsobject_sfk', 'rel_bag_nag_bag_vot_adresseert_verblijfsobject', 'bag_nummeraanduidingen', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_nag_bag_wps_ligt_in_woonplaats_dfk', 'rel_bag_nag_bag_wps_ligt_in_woonplaats', 'bag_woonplaatsen', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_nag_bag_wps_ligt_in_woonplaats_sfk', 'rel_bag_nag_bag_wps_ligt_in_woonplaats', 'bag_nummeraanduidingen', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_ore_bag_dsr_heeft_dossier_sfk', 'rel_bag_ore_bag_dsr_heeft_dossier', 'bag_openbareruimtes', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_ore_bag_dsr_heeft_dossier_dfk', 'rel_bag_ore_bag_dsr_heeft_dossier', 'bag_dossiers', ['dst_id'], ['_id'])
    op.create_foreign_key('rel_bag_ore_bag_wps_ligt_in_woonplaats_sfk', 'rel_bag_ore_bag_wps_ligt_in_woonplaats', 'bag_openbareruimtes', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_ore_bag_wps_ligt_in_woonplaats_dfk', 'rel_bag_ore_bag_wps_ligt_in_woonplaats', 'bag_woonplaatsen', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_pnd_bag_dsr_heeft_dossier_sfk', 'rel_bag_pnd_bag_dsr_heeft_dossier', 'bag_panden', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_pnd_bag_dsr_heeft_dossier_dfk', 'rel_bag_pnd_bag_dsr_heeft_dossier', 'bag_dossiers', ['dst_id'], ['_id'])
    op.create_foreign_key('rel_bag_pnd_gbd_bbk_ligt_in_bouwblok_dfk', 'rel_bag_pnd_gbd_bbk_ligt_in_bouwblok', 'gebieden_bouwblokken', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_pnd_gbd_bbk_ligt_in_bouwblok_sfk', 'rel_bag_pnd_gbd_bbk_ligt_in_bouwblok', 'bag_panden', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_sps_bag_dsr_heeft_dossier_sfk', 'rel_bag_sps_bag_dsr_heeft_dossier', 'bag_standplaatsen', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_sps_bag_dsr_heeft_dossier_dfk', 'rel_bag_sps_bag_dsr_heeft_dossier', 'bag_dossiers', ['dst_id'], ['_id'])
    op.create_foreign_key('rel_bag_sps_bag_nag_heeft_hoofdadres_sfk', 'rel_bag_sps_bag_nag_heeft_hoofdadres', 'bag_standplaatsen', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_sps_bag_nag_heeft_hoofdadres_dfk', 'rel_bag_sps_bag_nag_heeft_hoofdadres', 'bag_nummeraanduidingen', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_sps_bag_nag_heeft_nevenadres_sfk', 'rel_bag_sps_bag_nag_heeft_nevenadres', 'bag_standplaatsen', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_sps_bag_nag_heeft_nevenadres_dfk', 'rel_bag_sps_bag_nag_heeft_nevenadres', 'bag_nummeraanduidingen', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_sps_gbd_brt_ligt_in_buurt_dfk', 'rel_bag_sps_gbd_brt_ligt_in_buurt', 'gebieden_buurten', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_sps_gbd_brt_ligt_in_buurt_sfk', 'rel_bag_sps_gbd_brt_ligt_in_buurt', 'bag_standplaatsen', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_vot_bag_dsr_heeft_dossier_dfk', 'rel_bag_vot_bag_dsr_heeft_dossier', 'bag_dossiers', ['dst_id'], ['_id'])
    op.create_foreign_key('rel_bag_vot_bag_dsr_heeft_dossier_sfk', 'rel_bag_vot_bag_dsr_heeft_dossier', 'bag_verblijfsobjecten', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_vot_bag_nag_heeft_hoofdadres_sfk', 'rel_bag_vot_bag_nag_heeft_hoofdadres', 'bag_verblijfsobjecten', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_vot_bag_nag_heeft_hoofdadres_dfk', 'rel_bag_vot_bag_nag_heeft_hoofdadres', 'bag_nummeraanduidingen', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_vot_bag_nag_heeft_nevenadres_sfk', 'rel_bag_vot_bag_nag_heeft_nevenadres', 'bag_verblijfsobjecten', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_vot_bag_nag_heeft_nevenadres_dfk', 'rel_bag_vot_bag_nag_heeft_nevenadres', 'bag_nummeraanduidingen', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_vot_bag_pnd_ligt_in_panden_dfk', 'rel_bag_vot_bag_pnd_ligt_in_panden', 'bag_panden', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_vot_bag_pnd_ligt_in_panden_sfk', 'rel_bag_vot_bag_pnd_ligt_in_panden', 'bag_verblijfsobjecten', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_vot_gbd_brt_ligt_in_buurt_sfk', 'rel_bag_vot_gbd_brt_ligt_in_buurt', 'bag_verblijfsobjecten', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_vot_gbd_brt_ligt_in_buurt_dfk', 'rel_bag_vot_gbd_brt_ligt_in_buurt', 'gebieden_buurten', ['dst_id', 'dst_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_wps_bag_dsr_heeft_dossier_dfk', 'rel_bag_wps_bag_dsr_heeft_dossier', 'bag_dossiers', ['dst_id'], ['_id'])
    op.create_foreign_key('rel_bag_wps_bag_dsr_heeft_dossier_sfk', 'rel_bag_wps_bag_dsr_heeft_dossier', 'bag_woonplaatsen', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    op.create_foreign_key('rel_bag_wps_brk_gme_ligt_in_gemeente_dfk', 'rel_bag_wps_brk_gme_ligt_in_gemeente', 'brk_gemeentes', ['dst_id'], ['_id'])
    op.create_foreign_key('rel_bag_wps_brk_gme_ligt_in_gemeente_sfk', 'rel_bag_wps_brk_gme_ligt_in_gemeente', 'bag_woonplaatsen', ['src_id', 'src_volgnummer'], ['_id', 'volgnummer'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('rel_bag_wps_brk_gme_ligt_in_gemeente_sfk', 'rel_bag_wps_brk_gme_ligt_in_gemeente', type_='foreignkey')
    op.drop_constraint('rel_bag_wps_brk_gme_ligt_in_gemeente_dfk', 'rel_bag_wps_brk_gme_ligt_in_gemeente', type_='foreignkey')
    op.drop_constraint('rel_bag_wps_bag_dsr_heeft_dossier_sfk', 'rel_bag_wps_bag_dsr_heeft_dossier', type_='foreignkey')
    op.drop_constraint('rel_bag_wps_bag_dsr_heeft_dossier_dfk', 'rel_bag_wps_bag_dsr_heeft_dossier', type_='foreignkey')
    op.drop_constraint('rel_bag_vot_gbd_brt_ligt_in_buurt_dfk', 'rel_bag_vot_gbd_brt_ligt_in_buurt', type_='foreignkey')
    op.drop_constraint('rel_bag_vot_gbd_brt_ligt_in_buurt_sfk', 'rel_bag_vot_gbd_brt_ligt_in_buurt', type_='foreignkey')
    op.drop_constraint('rel_bag_vot_bag_pnd_ligt_in_panden_sfk', 'rel_bag_vot_bag_pnd_ligt_in_panden', type_='foreignkey')
    op.drop_constraint('rel_bag_vot_bag_pnd_ligt_in_panden_dfk', 'rel_bag_vot_bag_pnd_ligt_in_panden', type_='foreignkey')
    op.drop_constraint('rel_bag_vot_bag_nag_heeft_nevenadres_dfk', 'rel_bag_vot_bag_nag_heeft_nevenadres', type_='foreignkey')
    op.drop_constraint('rel_bag_vot_bag_nag_heeft_nevenadres_sfk', 'rel_bag_vot_bag_nag_heeft_nevenadres', type_='foreignkey')
    op.drop_constraint('rel_bag_vot_bag_nag_heeft_hoofdadres_dfk', 'rel_bag_vot_bag_nag_heeft_hoofdadres', type_='foreignkey')
    op.drop_constraint('rel_bag_vot_bag_nag_heeft_hoofdadres_sfk', 'rel_bag_vot_bag_nag_heeft_hoofdadres', type_='foreignkey')
    op.drop_constraint('rel_bag_vot_bag_dsr_heeft_dossier_sfk', 'rel_bag_vot_bag_dsr_heeft_dossier', type_='foreignkey')
    op.drop_constraint('rel_bag_vot_bag_dsr_heeft_dossier_dfk', 'rel_bag_vot_bag_dsr_heeft_dossier', type_='foreignkey')
    op.drop_constraint('rel_bag_sps_gbd_brt_ligt_in_buurt_sfk', 'rel_bag_sps_gbd_brt_ligt_in_buurt', type_='foreignkey')
    op.drop_constraint('rel_bag_sps_gbd_brt_ligt_in_buurt_dfk', 'rel_bag_sps_gbd_brt_ligt_in_buurt', type_='foreignkey')
    op.drop_constraint('rel_bag_sps_bag_nag_heeft_nevenadres_dfk', 'rel_bag_sps_bag_nag_heeft_nevenadres', type_='foreignkey')
    op.drop_constraint('rel_bag_sps_bag_nag_heeft_nevenadres_sfk', 'rel_bag_sps_bag_nag_heeft_nevenadres', type_='foreignkey')
    op.drop_constraint('rel_bag_sps_bag_nag_heeft_hoofdadres_dfk', 'rel_bag_sps_bag_nag_heeft_hoofdadres', type_='foreignkey')
    op.drop_constraint('rel_bag_sps_bag_nag_heeft_hoofdadres_sfk', 'rel_bag_sps_bag_nag_heeft_hoofdadres', type_='foreignkey')
    op.drop_constraint('rel_bag_sps_bag_dsr_heeft_dossier_dfk', 'rel_bag_sps_bag_dsr_heeft_dossier', type_='foreignkey')
    op.drop_constraint('rel_bag_sps_bag_dsr_heeft_dossier_sfk', 'rel_bag_sps_bag_dsr_heeft_dossier', type_='foreignkey')
    op.drop_constraint('rel_bag_pnd_gbd_bbk_ligt_in_bouwblok_sfk', 'rel_bag_pnd_gbd_bbk_ligt_in_bouwblok', type_='foreignkey')
    op.drop_constraint('rel_bag_pnd_gbd_bbk_ligt_in_bouwblok_dfk', 'rel_bag_pnd_gbd_bbk_ligt_in_bouwblok', type_='foreignkey')
    op.drop_constraint('rel_bag_pnd_bag_dsr_heeft_dossier_dfk', 'rel_bag_pnd_bag_dsr_heeft_dossier', type_='foreignkey')
    op.drop_constraint('rel_bag_pnd_bag_dsr_heeft_dossier_sfk', 'rel_bag_pnd_bag_dsr_heeft_dossier', type_='foreignkey')
    op.drop_constraint('rel_bag_ore_bag_wps_ligt_in_woonplaats_dfk', 'rel_bag_ore_bag_wps_ligt_in_woonplaats', type_='foreignkey')
    op.drop_constraint('rel_bag_ore_bag_wps_ligt_in_woonplaats_sfk', 'rel_bag_ore_bag_wps_ligt_in_woonplaats', type_='foreignkey')
    op.drop_constraint('rel_bag_ore_bag_dsr_heeft_dossier_dfk', 'rel_bag_ore_bag_dsr_heeft_dossier', type_='foreignkey')
    op.drop_constraint('rel_bag_ore_bag_dsr_heeft_dossier_sfk', 'rel_bag_ore_bag_dsr_heeft_dossier', type_='foreignkey')
    op.drop_constraint('rel_bag_nag_bag_wps_ligt_in_woonplaats_sfk', 'rel_bag_nag_bag_wps_ligt_in_woonplaats', type_='foreignkey')
    op.drop_constraint('rel_bag_nag_bag_wps_ligt_in_woonplaats_dfk', 'rel_bag_nag_bag_wps_ligt_in_woonplaats', type_='foreignkey')
    op.drop_constraint('rel_bag_nag_bag_vot_adresseert_verblijfsobject_sfk', 'rel_bag_nag_bag_vot_adresseert_verblijfsobject', type_='foreignkey')
    op.drop_constraint('rel_bag_nag_bag_vot_adresseert_verblijfsobject_dfk', 'rel_bag_nag_bag_vot_adresseert_verblijfsobject', type_='foreignkey')
    op.drop_constraint('rel_bag_nag_bag_sps_adresseert_standplaats_dfk', 'rel_bag_nag_bag_sps_adresseert_standplaats', type_='foreignkey')
    op.drop_constraint('rel_bag_nag_bag_sps_adresseert_standplaats_sfk', 'rel_bag_nag_bag_sps_adresseert_standplaats', type_='foreignkey')
    op.drop_constraint('rel_bag_nag_bag_ore_ligt_aan_openbareruimte_sfk', 'rel_bag_nag_bag_ore_ligt_aan_openbareruimte', type_='foreignkey')
    op.drop_constraint('rel_bag_nag_bag_ore_ligt_aan_openbareruimte_dfk', 'rel_bag_nag_bag_ore_ligt_aan_openbareruimte', type_='foreignkey')
    op.drop_constraint('rel_bag_nag_bag_lps_adresseert_ligplaats_dfk', 'rel_bag_nag_bag_lps_adresseert_ligplaats', type_='foreignkey')
    op.drop_constraint('rel_bag_nag_bag_lps_adresseert_ligplaats_sfk', 'rel_bag_nag_bag_lps_adresseert_ligplaats', type_='foreignkey')
    op.drop_constraint('rel_bag_nag_bag_dsr_heeft_dossier_dfk', 'rel_bag_nag_bag_dsr_heeft_dossier', type_='foreignkey')
    op.drop_constraint('rel_bag_nag_bag_dsr_heeft_dossier_sfk', 'rel_bag_nag_bag_dsr_heeft_dossier', type_='foreignkey')
    op.drop_constraint('rel_bag_lps_gbd_brt_ligt_in_buurt_sfk', 'rel_bag_lps_gbd_brt_ligt_in_buurt', type_='foreignkey')
    op.drop_constraint('rel_bag_lps_gbd_brt_ligt_in_buurt_dfk', 'rel_bag_lps_gbd_brt_ligt_in_buurt', type_='foreignkey')
    op.drop_constraint('rel_bag_lps_bag_nag_heeft_nevenadres_dfk', 'rel_bag_lps_bag_nag_heeft_nevenadres', type_='foreignkey')
    op.drop_constraint('rel_bag_lps_bag_nag_heeft_nevenadres_sfk', 'rel_bag_lps_bag_nag_heeft_nevenadres', type_='foreignkey')
    op.drop_constraint('rel_bag_lps_bag_nag_heeft_hoofdadres_dfk', 'rel_bag_lps_bag_nag_heeft_hoofdadres', type_='foreignkey')
    op.drop_constraint('rel_bag_lps_bag_nag_heeft_hoofdadres_sfk', 'rel_bag_lps_bag_nag_heeft_hoofdadres', type_='foreignkey')
    op.drop_constraint('rel_bag_lps_bag_dsr_heeft_dossier_dfk', 'rel_bag_lps_bag_dsr_heeft_dossier', type_='foreignkey')
    op.drop_constraint('rel_bag_lps_bag_dsr_heeft_dossier_sfk', 'rel_bag_lps_bag_dsr_heeft_dossier', type_='foreignkey')
    op.drop_constraint('rel_bag_dsr_bag_bdt_heeft_brondocumenten_sfk', 'rel_bag_dsr_bag_bdt_heeft_brondocumenten', type_='foreignkey')
    op.drop_constraint('rel_bag_dsr_bag_bdt_heeft_brondocumenten_dfk', 'rel_bag_dsr_bag_bdt_heeft_brondocumenten', type_='foreignkey')
    # ### end Alembic commands ###