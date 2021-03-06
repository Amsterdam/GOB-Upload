"""Add expiration date

Revision ID: 2db90c270904
Revises: 47b65594bcf1
Create Date: 2019-04-15 13:32:04.056860

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '2db90c270904'
down_revision = '47b65594bcf1'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('bag_ligplaatsen', sa.Column('feitelijk_gebruik', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    op.add_column('bag_standplaatsen', sa.Column('feitelijk_gebruik', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))

    op.add_column('bag_brondocumenten', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('bag_ligplaatsen', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('bag_nummeraanduidingen', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('bag_openbareruimtes', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('bag_panden', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('bag_standplaatsen', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('bag_verblijfsobjecten', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('bag_woonplaatsen', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('gebieden_bouwblokken', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('gebieden_buurten', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('gebieden_ggpgebieden', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('gebieden_ggwgebieden', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('gebieden_stadsdelen', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('gebieden_wijken', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('meetbouten_meetbouten', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('meetbouten_metingen', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('meetbouten_referentiepunten', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('meetbouten_rollagen', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('nap_peilmerken', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_lps_bag_bdt_heeft_brondocumenten', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_lps_bag_nag_heeft_hoofdadres', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_lps_bag_nag_heeft_nevenadres', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_lps_gbd_brt_ligt_in_buurt', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_nag_bag_bdt_heeft_brondocumenten', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_nag_bag_lps_adresseert_ligplaats', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_nag_bag_ore_ligt_aan_openbareruimte', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_nag_bag_sps_adresseert_standplaats', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_nag_bag_vot_adresseert_verblijfsobject', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_nag_bag_wps_ligt_in_woonplaats', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_ore_bag_bdt_heeft_brondocumenten', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_ore_bag_wps_ligt_in_woonplaats', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_pnd_bag_bdt_heeft_brondocumenten', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_pnd_gbd_bbk_ligt_in_bouwblok', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_sps_bag_bdt_heeft_brondocumenten', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_sps_bag_nag_heeft_hoofdadres', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_sps_bag_nag_heeft_nevenadres', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_sps_gbd_brt_ligt_in_buurt', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_vot_bag_bdt_heeft_brondocumenten', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_vot_bag_nag_heeft_hoofdadres', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_vot_bag_nag_heeft_nevenadres', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_vot_bag_pnd_ligt_in_panden', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_vot_gbd_brt_ligt_in_buurt', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_bag_wps_bag_bdt_heeft_brondocumenten', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_gbd_bbk_gbd_brt_ligt_in_buurt', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_gbd_brt_gbd_ggp__ligt_in_ggpgebied', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_gbd_brt_gbd_ggw__ligt_in_ggwgebied', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_gbd_brt_gbd_wijk_ligt_in_wijk', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_gbd_ggp_gbd_brt_bestaat_uit_buurten', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_gbd_ggp_gbd_sdl_ligt_in_stadsdeel', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_gbd_ggw_gbd_brt_bestaat_uit_buurten', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_gbd_ggw_gbd_sdl_ligt_in_stadsdeel', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_gbd_wijk_gbd_ggw__ligt_in_ggwgebied', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_gbd_wijk_gbd_sdl_ligt_in_stadsdeel', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_mbn_mbt_bag_nag_nabij_nummeraanduiding', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_mbn_mbt_gbd_bbk_ligt_in_bouwblok', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_mbn_mbt_gbd_brt_ligt_in_buurt', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_mbn_mbt_gbd_sdl_ligt_in_stadsdeel', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_mbn_mtg_mbn_mbt_hoort_bij_meetbout', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_mbn_mtg_mbn_rpt_refereert_aan_referentiepunten', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_mbn_rlg_gbd_bbk_is_gemeten_van_bouwblok', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_mbn_rpt_bag_nag_nabij_nummeraanduiding', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_mbn_rpt_gbd_bbk_ligt_in_bouwblok', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_mbn_rpt_gbd_brt_ligt_in_buurt', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_mbn_rpt_gbd_sdl_ligt_in_stadsdeel', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_mbn_rpt_nap_pmk_is_nap_peilmerk', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('rel_nap_pmk_gbd_bbk_ligt_in_bouwblok', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('test_catalogue_test_entity', sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('bag_ligplaatsen', 'feitelijk_gebruik')
    op.drop_column('bag_standplaatsen', 'feitelijk_gebruik')

    op.drop_column('test_catalogue_test_entity', '_expiration_date')
    op.drop_column('rel_nap_pmk_gbd_bbk_ligt_in_bouwblok', '_expiration_date')
    op.drop_column('rel_mbn_rpt_nap_pmk_is_nap_peilmerk', '_expiration_date')
    op.drop_column('rel_mbn_rpt_gbd_sdl_ligt_in_stadsdeel', '_expiration_date')
    op.drop_column('rel_mbn_rpt_gbd_brt_ligt_in_buurt', '_expiration_date')
    op.drop_column('rel_mbn_rpt_gbd_bbk_ligt_in_bouwblok', '_expiration_date')
    op.drop_column('rel_mbn_rpt_bag_nag_nabij_nummeraanduiding', '_expiration_date')
    op.drop_column('rel_mbn_rlg_gbd_bbk_is_gemeten_van_bouwblok', '_expiration_date')
    op.drop_column('rel_mbn_mtg_mbn_rpt_refereert_aan_referentiepunten', '_expiration_date')
    op.drop_column('rel_mbn_mtg_mbn_mbt_hoort_bij_meetbout', '_expiration_date')
    op.drop_column('rel_mbn_mbt_gbd_sdl_ligt_in_stadsdeel', '_expiration_date')
    op.drop_column('rel_mbn_mbt_gbd_brt_ligt_in_buurt', '_expiration_date')
    op.drop_column('rel_mbn_mbt_gbd_bbk_ligt_in_bouwblok', '_expiration_date')
    op.drop_column('rel_mbn_mbt_bag_nag_nabij_nummeraanduiding', '_expiration_date')
    op.drop_column('rel_gbd_wijk_gbd_sdl_ligt_in_stadsdeel', '_expiration_date')
    op.drop_column('rel_gbd_wijk_gbd_ggw__ligt_in_ggwgebied', '_expiration_date')
    op.drop_column('rel_gbd_ggw_gbd_sdl_ligt_in_stadsdeel', '_expiration_date')
    op.drop_column('rel_gbd_ggw_gbd_brt_bestaat_uit_buurten', '_expiration_date')
    op.drop_column('rel_gbd_ggp_gbd_sdl_ligt_in_stadsdeel', '_expiration_date')
    op.drop_column('rel_gbd_ggp_gbd_brt_bestaat_uit_buurten', '_expiration_date')
    op.drop_column('rel_gbd_brt_gbd_wijk_ligt_in_wijk', '_expiration_date')
    op.drop_column('rel_gbd_brt_gbd_ggw__ligt_in_ggwgebied', '_expiration_date')
    op.drop_column('rel_gbd_brt_gbd_ggp__ligt_in_ggpgebied', '_expiration_date')
    op.drop_column('rel_gbd_bbk_gbd_brt_ligt_in_buurt', '_expiration_date')
    op.drop_column('rel_bag_wps_bag_bdt_heeft_brondocumenten', '_expiration_date')
    op.drop_column('rel_bag_vot_gbd_brt_ligt_in_buurt', '_expiration_date')
    op.drop_column('rel_bag_vot_bag_pnd_ligt_in_panden', '_expiration_date')
    op.drop_column('rel_bag_vot_bag_nag_heeft_nevenadres', '_expiration_date')
    op.drop_column('rel_bag_vot_bag_nag_heeft_hoofdadres', '_expiration_date')
    op.drop_column('rel_bag_vot_bag_bdt_heeft_brondocumenten', '_expiration_date')
    op.drop_column('rel_bag_sps_gbd_brt_ligt_in_buurt', '_expiration_date')
    op.drop_column('rel_bag_sps_bag_nag_heeft_nevenadres', '_expiration_date')
    op.drop_column('rel_bag_sps_bag_nag_heeft_hoofdadres', '_expiration_date')
    op.drop_column('rel_bag_sps_bag_bdt_heeft_brondocumenten', '_expiration_date')
    op.drop_column('rel_bag_pnd_gbd_bbk_ligt_in_bouwblok', '_expiration_date')
    op.drop_column('rel_bag_pnd_bag_bdt_heeft_brondocumenten', '_expiration_date')
    op.drop_column('rel_bag_ore_bag_wps_ligt_in_woonplaats', '_expiration_date')
    op.drop_column('rel_bag_ore_bag_bdt_heeft_brondocumenten', '_expiration_date')
    op.drop_column('rel_bag_nag_bag_wps_ligt_in_woonplaats', '_expiration_date')
    op.drop_column('rel_bag_nag_bag_vot_adresseert_verblijfsobject', '_expiration_date')
    op.drop_column('rel_bag_nag_bag_sps_adresseert_standplaats', '_expiration_date')
    op.drop_column('rel_bag_nag_bag_ore_ligt_aan_openbareruimte', '_expiration_date')
    op.drop_column('rel_bag_nag_bag_lps_adresseert_ligplaats', '_expiration_date')
    op.drop_column('rel_bag_nag_bag_bdt_heeft_brondocumenten', '_expiration_date')
    op.drop_column('rel_bag_lps_gbd_brt_ligt_in_buurt', '_expiration_date')
    op.drop_column('rel_bag_lps_bag_nag_heeft_nevenadres', '_expiration_date')
    op.drop_column('rel_bag_lps_bag_nag_heeft_hoofdadres', '_expiration_date')
    op.drop_column('rel_bag_lps_bag_bdt_heeft_brondocumenten', '_expiration_date')
    op.drop_column('nap_peilmerken', '_expiration_date')
    op.drop_column('meetbouten_rollagen', '_expiration_date')
    op.drop_column('meetbouten_referentiepunten', '_expiration_date')
    op.drop_column('meetbouten_metingen', '_expiration_date')
    op.drop_column('meetbouten_meetbouten', '_expiration_date')
    op.drop_column('gebieden_wijken', '_expiration_date')
    op.drop_column('gebieden_stadsdelen', '_expiration_date')
    op.drop_column('gebieden_ggwgebieden', '_expiration_date')
    op.drop_column('gebieden_ggpgebieden', '_expiration_date')
    op.drop_column('gebieden_buurten', '_expiration_date')
    op.drop_column('gebieden_bouwblokken', '_expiration_date')
    op.drop_column('bag_woonplaatsen', '_expiration_date')
    op.drop_column('bag_verblijfsobjecten', '_expiration_date')
    op.drop_column('bag_standplaatsen', '_expiration_date')
    op.drop_column('bag_panden', '_expiration_date')
    op.drop_column('bag_openbareruimtes', '_expiration_date')
    op.drop_column('bag_nummeraanduidingen', '_expiration_date')
    op.drop_column('bag_ligplaatsen', '_expiration_date')
    op.drop_column('bag_brondocumenten', '_expiration_date')
    # ### end Alembic commands ###
