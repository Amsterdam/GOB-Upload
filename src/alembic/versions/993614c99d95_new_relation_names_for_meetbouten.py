"""new relation names for meetbouten

Revision ID: 993614c99d95
Revises: cda2005bcf0e
Create Date: 2022-10-27 14:25:33.468903

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '993614c99d95'
down_revision = 'cda2005bcf0e'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute('DROP TABLE rel_mbn_mtg_mbn_rpt__rft_n__meetbouten_referentiepunten CASCADE')
    op.create_table('rel_mbn_mtg_mbn_rpt__meetbouten_refpnt_rft_aan_',
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
    sa.ForeignKeyConstraint(['dst_id'], ['meetbouten_referentiepunten._id'], name='rel_mbn_mtg_mbn_rpt__meetbouten_refpnt_rft_aan__dfk'),
    sa.ForeignKeyConstraint(['src_id'], ['meetbouten_metingen._id'], name='rel_mbn_mtg_mbn_rpt__meetbouten_refpnt_rft_aan__sfk'),
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_source_id', name='rel_mbn_mtg_mbn_rpt__meetbouten_refpnt_rft_aan__uniq'),
    sa.UniqueConstraint('_tid', name='rel_mbn_mtg_mbn_rpt__meetbouten_refpnt_rft_aan___tid_key')
    )
    op.drop_table('rel_mbn_mbt_bag_nag_nabij_nummeraanduiding')
    op.drop_table('rel_mbn_rpt_bag_nag_nabij_nummeraanduiding')
    op.drop_constraint('rel_gbd_bbk_brk_gme_ligt_in_gemeente__tid_key', 'rel_gbd_bbk_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.drop_constraint('rel_gbd_bbk_brk_gme_ligt_in_gemeente_uniq', 'rel_gbd_bbk_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.create_unique_constraint('rel_gbd_bbk_brk_gme_ligt_in_brk_gemeente__tid_key', 'rel_gbd_bbk_brk_gme_ligt_in_brk_gemeente', ['_tid'])
    op.create_unique_constraint('rel_gbd_bbk_brk_gme_ligt_in_brk_gemeente_uniq', 'rel_gbd_bbk_brk_gme_ligt_in_brk_gemeente', ['_source_id'])
    op.drop_constraint('rel_gbd_bbk_gbd_brt_ligt_in_buurt__tid_key', 'rel_gbd_bbk_gbd_brt_ligt_in_gebieden_buurt', type_='unique')
    op.drop_constraint('rel_gbd_bbk_gbd_brt_ligt_in_buurt_uniq', 'rel_gbd_bbk_gbd_brt_ligt_in_gebieden_buurt', type_='unique')
    op.create_unique_constraint('rel_gbd_bbk_gbd_brt_ligt_in_gebieden_buurt__tid_key', 'rel_gbd_bbk_gbd_brt_ligt_in_gebieden_buurt', ['_tid'])
    op.create_unique_constraint('rel_gbd_bbk_gbd_brt_ligt_in_gebieden_buurt_uniq', 'rel_gbd_bbk_gbd_brt_ligt_in_gebieden_buurt', ['_source_id'])
    op.drop_constraint('rel_gbd_brt_brk_gme_ligt_in_gemeente__tid_key', 'rel_gbd_brt_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.drop_constraint('rel_gbd_brt_brk_gme_ligt_in_gemeente_uniq', 'rel_gbd_brt_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.create_unique_constraint('rel_gbd_brt_brk_gme_ligt_in_brk_gemeente__tid_key', 'rel_gbd_brt_brk_gme_ligt_in_brk_gemeente', ['_tid'])
    op.create_unique_constraint('rel_gbd_brt_brk_gme_ligt_in_brk_gemeente_uniq', 'rel_gbd_brt_brk_gme_ligt_in_brk_gemeente', ['_source_id'])
    op.drop_constraint('rel_gbd_brt_gbd_ggp_ligt_in_ggpgebied__tid_key', 'rel_gbd_brt_gbd_ggp_ligt_in_gebieden_ggpgebied', type_='unique')
    op.drop_constraint('rel_gbd_brt_gbd_ggp_ligt_in_ggpgebied_uniq', 'rel_gbd_brt_gbd_ggp_ligt_in_gebieden_ggpgebied', type_='unique')
    op.create_unique_constraint('rel_gbd_brt_gbd_ggp_ligt_in_gebieden_ggpgebied__tid_key', 'rel_gbd_brt_gbd_ggp_ligt_in_gebieden_ggpgebied', ['_tid'])
    op.create_unique_constraint('rel_gbd_brt_gbd_ggp_ligt_in_gebieden_ggpgebied_uniq', 'rel_gbd_brt_gbd_ggp_ligt_in_gebieden_ggpgebied', ['_source_id'])
    op.drop_constraint('rel_gbd_brt_gbd_ggw_ligt_in_ggwgebied__tid_key', 'rel_gbd_brt_gbd_ggw_ligt_in_gebieden_ggwgebied', type_='unique')
    op.drop_constraint('rel_gbd_brt_gbd_ggw_ligt_in_ggwgebied_uniq', 'rel_gbd_brt_gbd_ggw_ligt_in_gebieden_ggwgebied', type_='unique')
    op.create_unique_constraint('rel_gbd_brt_gbd_ggw_ligt_in_gebieden_ggwgebied__tid_key', 'rel_gbd_brt_gbd_ggw_ligt_in_gebieden_ggwgebied', ['_tid'])
    op.create_unique_constraint('rel_gbd_brt_gbd_ggw_ligt_in_gebieden_ggwgebied_uniq', 'rel_gbd_brt_gbd_ggw_ligt_in_gebieden_ggwgebied', ['_source_id'])
    op.drop_constraint('rel_gbd_brt_gbd_wijk_ligt_in_wijk__tid_key', 'rel_gbd_brt_gbd_wijk_ligt_in_gebieden_wijk', type_='unique')
    op.drop_constraint('rel_gbd_brt_gbd_wijk_ligt_in_wijk_uniq', 'rel_gbd_brt_gbd_wijk_ligt_in_gebieden_wijk', type_='unique')
    op.create_unique_constraint('rel_gbd_brt_gbd_wijk_ligt_in_gebieden_wijk__tid_key', 'rel_gbd_brt_gbd_wijk_ligt_in_gebieden_wijk', ['_tid'])
    op.create_unique_constraint('rel_gbd_brt_gbd_wijk_ligt_in_gebieden_wijk_uniq', 'rel_gbd_brt_gbd_wijk_ligt_in_gebieden_wijk', ['_source_id'])
    op.drop_constraint('rel_gbd_ggp_brk_gme_ligt_in_gemeente__tid_key', 'rel_gbd_ggp_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.drop_constraint('rel_gbd_ggp_brk_gme_ligt_in_gemeente_uniq', 'rel_gbd_ggp_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.create_unique_constraint('rel_gbd_ggp_brk_gme_ligt_in_brk_gemeente__tid_key', 'rel_gbd_ggp_brk_gme_ligt_in_brk_gemeente', ['_tid'])
    op.create_unique_constraint('rel_gbd_ggp_brk_gme_ligt_in_brk_gemeente_uniq', 'rel_gbd_ggp_brk_gme_ligt_in_brk_gemeente', ['_source_id'])
    op.drop_constraint('rel_gbd_ggp_gbd_brt_bestaat_uit_buurten__tid_key', 'rel_gbd_ggp_gbd_brt_bestaat_uit_gebieden_buurten', type_='unique')
    op.drop_constraint('rel_gbd_ggp_gbd_brt_bestaat_uit_buurten_uniq', 'rel_gbd_ggp_gbd_brt_bestaat_uit_gebieden_buurten', type_='unique')
    op.create_unique_constraint('rel_gbd_ggp_gbd_brt_bestaat_uit_gebieden_buurten__tid_key', 'rel_gbd_ggp_gbd_brt_bestaat_uit_gebieden_buurten', ['_tid'])
    op.create_unique_constraint('rel_gbd_ggp_gbd_brt_bestaat_uit_gebieden_buurten_uniq', 'rel_gbd_ggp_gbd_brt_bestaat_uit_gebieden_buurten', ['_source_id'])
    op.drop_constraint('rel_gbd_ggp_gbd_sdl_ligt_in_stadsdeel__tid_key', 'rel_gbd_ggp_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.drop_constraint('rel_gbd_ggp_gbd_sdl_ligt_in_stadsdeel_uniq', 'rel_gbd_ggp_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.create_unique_constraint('rel_gbd_ggp_gbd_sdl_ligt_in_gebieden_stadsdeel__tid_key', 'rel_gbd_ggp_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_tid'])
    op.create_unique_constraint('rel_gbd_ggp_gbd_sdl_ligt_in_gebieden_stadsdeel_uniq', 'rel_gbd_ggp_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_source_id'])
    op.drop_constraint('rel_gbd_ggw_brk_gme_ligt_in_gemeente__tid_key', 'rel_gbd_ggw_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.drop_constraint('rel_gbd_ggw_brk_gme_ligt_in_gemeente_uniq', 'rel_gbd_ggw_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.create_unique_constraint('rel_gbd_ggw_brk_gme_ligt_in_brk_gemeente__tid_key', 'rel_gbd_ggw_brk_gme_ligt_in_brk_gemeente', ['_tid'])
    op.create_unique_constraint('rel_gbd_ggw_brk_gme_ligt_in_brk_gemeente_uniq', 'rel_gbd_ggw_brk_gme_ligt_in_brk_gemeente', ['_source_id'])
    op.drop_constraint('rel_gbd_ggw_gbd_brt_bestaat_uit_buurten__tid_key', 'rel_gbd_ggw_gbd_brt_bestaat_uit_gebieden_buurten', type_='unique')
    op.drop_constraint('rel_gbd_ggw_gbd_brt_bestaat_uit_buurten_uniq', 'rel_gbd_ggw_gbd_brt_bestaat_uit_gebieden_buurten', type_='unique')
    op.create_unique_constraint('rel_gbd_ggw_gbd_brt_bestaat_uit_gebieden_buurten__tid_key', 'rel_gbd_ggw_gbd_brt_bestaat_uit_gebieden_buurten', ['_tid'])
    op.create_unique_constraint('rel_gbd_ggw_gbd_brt_bestaat_uit_gebieden_buurten_uniq', 'rel_gbd_ggw_gbd_brt_bestaat_uit_gebieden_buurten', ['_source_id'])
    op.drop_constraint('rel_gbd_ggw_gbd_sdl_ligt_in_stadsdeel__tid_key', 'rel_gbd_ggw_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.drop_constraint('rel_gbd_ggw_gbd_sdl_ligt_in_stadsdeel_uniq', 'rel_gbd_ggw_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.create_unique_constraint('rel_gbd_ggw_gbd_sdl_ligt_in_gebieden_stadsdeel__tid_key', 'rel_gbd_ggw_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_tid'])
    op.create_unique_constraint('rel_gbd_ggw_gbd_sdl_ligt_in_gebieden_stadsdeel_uniq', 'rel_gbd_ggw_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_source_id'])
    op.drop_constraint('rel_gbd_sdl_brk_gme_ligt_in_gemeente__tid_key', 'rel_gbd_sdl_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.drop_constraint('rel_gbd_sdl_brk_gme_ligt_in_gemeente_uniq', 'rel_gbd_sdl_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.create_unique_constraint('rel_gbd_sdl_brk_gme_ligt_in_brk_gemeente__tid_key', 'rel_gbd_sdl_brk_gme_ligt_in_brk_gemeente', ['_tid'])
    op.create_unique_constraint('rel_gbd_sdl_brk_gme_ligt_in_brk_gemeente_uniq', 'rel_gbd_sdl_brk_gme_ligt_in_brk_gemeente', ['_source_id'])
    op.drop_constraint('rel_gbd_wijk_brk_gme_ligt_in_gemeente__tid_key', 'rel_gbd_wijk_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.drop_constraint('rel_gbd_wijk_brk_gme_ligt_in_gemeente_uniq', 'rel_gbd_wijk_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.create_unique_constraint('rel_gbd_wijk_brk_gme_ligt_in_brk_gemeente__tid_key', 'rel_gbd_wijk_brk_gme_ligt_in_brk_gemeente', ['_tid'])
    op.create_unique_constraint('rel_gbd_wijk_brk_gme_ligt_in_brk_gemeente_uniq', 'rel_gbd_wijk_brk_gme_ligt_in_brk_gemeente', ['_source_id'])
    op.drop_constraint('rel_gbd_wijk_gbd_ggw_ligt_in_ggwgebied__tid_key', 'rel_gbd_wijk_gbd_ggw_ligt_in_gebieden_ggwgebied', type_='unique')
    op.drop_constraint('rel_gbd_wijk_gbd_ggw_ligt_in_ggwgebied_uniq', 'rel_gbd_wijk_gbd_ggw_ligt_in_gebieden_ggwgebied', type_='unique')
    op.create_unique_constraint('rel_gbd_wijk_gbd_ggw_ligt_in_gebieden_ggwgebied__tid_key', 'rel_gbd_wijk_gbd_ggw_ligt_in_gebieden_ggwgebied', ['_tid'])
    op.create_unique_constraint('rel_gbd_wijk_gbd_ggw_ligt_in_gebieden_ggwgebied_uniq', 'rel_gbd_wijk_gbd_ggw_ligt_in_gebieden_ggwgebied', ['_source_id'])
    op.drop_constraint('rel_gbd_wijk_gbd_sdl_ligt_in_stadsdeel__tid_key', 'rel_gbd_wijk_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.drop_constraint('rel_gbd_wijk_gbd_sdl_ligt_in_stadsdeel_uniq', 'rel_gbd_wijk_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.create_unique_constraint('rel_gbd_wijk_gbd_sdl_ligt_in_gebieden_stadsdeel__tid_key', 'rel_gbd_wijk_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_tid'])
    op.create_unique_constraint('rel_gbd_wijk_gbd_sdl_ligt_in_gebieden_stadsdeel_uniq', 'rel_gbd_wijk_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_source_id'])
    op.drop_constraint('rel_mbn_mbt_gbd_bbk_ligt_in_bouwblok__tid_key', 'rel_mbn_mbt_gbd_bbk_ligt_in_gebieden_bouwblok', type_='unique')
    op.drop_constraint('rel_mbn_mbt_gbd_bbk_ligt_in_bouwblok_uniq', 'rel_mbn_mbt_gbd_bbk_ligt_in_gebieden_bouwblok', type_='unique')
    op.create_unique_constraint('rel_mbn_mbt_gbd_bbk_ligt_in_gebieden_bouwblok__tid_key', 'rel_mbn_mbt_gbd_bbk_ligt_in_gebieden_bouwblok', ['_tid'])
    op.create_unique_constraint('rel_mbn_mbt_gbd_bbk_ligt_in_gebieden_bouwblok_uniq', 'rel_mbn_mbt_gbd_bbk_ligt_in_gebieden_bouwblok', ['_source_id'])
    op.drop_constraint('rel_mbn_mbt_gbd_brt_ligt_in_buurt__tid_key', 'rel_mbn_mbt_gbd_brt_ligt_in_gebieden_buurt', type_='unique')
    op.drop_constraint('rel_mbn_mbt_gbd_brt_ligt_in_buurt_uniq', 'rel_mbn_mbt_gbd_brt_ligt_in_gebieden_buurt', type_='unique')
    op.create_unique_constraint('rel_mbn_mbt_gbd_brt_ligt_in_gebieden_buurt__tid_key', 'rel_mbn_mbt_gbd_brt_ligt_in_gebieden_buurt', ['_tid'])
    op.create_unique_constraint('rel_mbn_mbt_gbd_brt_ligt_in_gebieden_buurt_uniq', 'rel_mbn_mbt_gbd_brt_ligt_in_gebieden_buurt', ['_source_id'])
    op.drop_constraint('rel_mbn_mbt_gbd_sdl_ligt_in_stadsdeel__tid_key', 'rel_mbn_mbt_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.drop_constraint('rel_mbn_mbt_gbd_sdl_ligt_in_stadsdeel_uniq', 'rel_mbn_mbt_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.create_unique_constraint('rel_mbn_mbt_gbd_sdl_ligt_in_gebieden_stadsdeel__tid_key', 'rel_mbn_mbt_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_tid'])
    op.create_unique_constraint('rel_mbn_mbt_gbd_sdl_ligt_in_gebieden_stadsdeel_uniq', 'rel_mbn_mbt_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_source_id'])
    op.drop_constraint('rel_mbn_mtg_mbn_mbt_hoort_bij_meetbout__tid_key', 'rel_mbn_mtg_mbn_mbt_hoort_bij_meetbouten_meetbout', type_='unique')
    op.drop_constraint('rel_mbn_mtg_mbn_mbt_hoort_bij_meetbout_uniq', 'rel_mbn_mtg_mbn_mbt_hoort_bij_meetbouten_meetbout', type_='unique')
    op.create_unique_constraint('rel_mbn_mtg_mbn_mbt_hoort_bij_meetbouten_meetbout__tid_key', 'rel_mbn_mtg_mbn_mbt_hoort_bij_meetbouten_meetbout', ['_tid'])
    op.create_unique_constraint('rel_mbn_mtg_mbn_mbt_hoort_bij_meetbouten_meetbout_uniq', 'rel_mbn_mtg_mbn_mbt_hoort_bij_meetbouten_meetbout', ['_source_id'])
    op.drop_constraint('rel_mbn_rlg_gbd_bbk_is_gemeten_van_bouwblok__tid_key', 'rel_mbn_rlg_gbd_bbk_is_gemeten_van_gebieden_bouwblok', type_='unique')
    op.drop_constraint('rel_mbn_rlg_gbd_bbk_is_gemeten_van_bouwblok_uniq', 'rel_mbn_rlg_gbd_bbk_is_gemeten_van_gebieden_bouwblok', type_='unique')
    op.create_unique_constraint('rel_mbn_rlg_gbd_bbk_is_gemeten_van_gebieden_bouwblok__tid_key', 'rel_mbn_rlg_gbd_bbk_is_gemeten_van_gebieden_bouwblok', ['_tid'])
    op.create_unique_constraint('rel_mbn_rlg_gbd_bbk_is_gemeten_van_gebieden_bouwblok_uniq', 'rel_mbn_rlg_gbd_bbk_is_gemeten_van_gebieden_bouwblok', ['_source_id'])
    op.drop_constraint('rel_mbn_rpt_gbd_bbk_ligt_in_bouwblok__tid_key', 'rel_mbn_rpt_gbd_bbk_ligt_in_gebieden_bouwblok', type_='unique')
    op.drop_constraint('rel_mbn_rpt_gbd_bbk_ligt_in_bouwblok_uniq', 'rel_mbn_rpt_gbd_bbk_ligt_in_gebieden_bouwblok', type_='unique')
    op.create_unique_constraint('rel_mbn_rpt_gbd_bbk_ligt_in_gebieden_bouwblok__tid_key', 'rel_mbn_rpt_gbd_bbk_ligt_in_gebieden_bouwblok', ['_tid'])
    op.create_unique_constraint('rel_mbn_rpt_gbd_bbk_ligt_in_gebieden_bouwblok_uniq', 'rel_mbn_rpt_gbd_bbk_ligt_in_gebieden_bouwblok', ['_source_id'])
    op.drop_constraint('rel_mbn_rpt_gbd_brt_ligt_in_buurt__tid_key', 'rel_mbn_rpt_gbd_brt_ligt_in_gebieden_buurt', type_='unique')
    op.drop_constraint('rel_mbn_rpt_gbd_brt_ligt_in_buurt_uniq', 'rel_mbn_rpt_gbd_brt_ligt_in_gebieden_buurt', type_='unique')
    op.create_unique_constraint('rel_mbn_rpt_gbd_brt_ligt_in_gebieden_buurt__tid_key', 'rel_mbn_rpt_gbd_brt_ligt_in_gebieden_buurt', ['_tid'])
    op.create_unique_constraint('rel_mbn_rpt_gbd_brt_ligt_in_gebieden_buurt_uniq', 'rel_mbn_rpt_gbd_brt_ligt_in_gebieden_buurt', ['_source_id'])
    op.drop_constraint('rel_mbn_rpt_gbd_sdl_ligt_in_stadsdeel__tid_key', 'rel_mbn_rpt_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.drop_constraint('rel_mbn_rpt_gbd_sdl_ligt_in_stadsdeel_uniq', 'rel_mbn_rpt_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.create_unique_constraint('rel_mbn_rpt_gbd_sdl_ligt_in_gebieden_stadsdeel__tid_key', 'rel_mbn_rpt_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_tid'])
    op.create_unique_constraint('rel_mbn_rpt_gbd_sdl_ligt_in_gebieden_stadsdeel_uniq', 'rel_mbn_rpt_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_source_id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('rel_mbn_rpt_gbd_sdl_ligt_in_gebieden_stadsdeel_uniq', 'rel_mbn_rpt_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.drop_constraint('rel_mbn_rpt_gbd_sdl_ligt_in_gebieden_stadsdeel__tid_key', 'rel_mbn_rpt_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.create_unique_constraint('rel_mbn_rpt_gbd_sdl_ligt_in_stadsdeel_uniq', 'rel_mbn_rpt_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_source_id'])
    op.create_unique_constraint('rel_mbn_rpt_gbd_sdl_ligt_in_stadsdeel__tid_key', 'rel_mbn_rpt_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_tid'])
    op.drop_constraint('rel_mbn_rpt_gbd_brt_ligt_in_gebieden_buurt_uniq', 'rel_mbn_rpt_gbd_brt_ligt_in_gebieden_buurt', type_='unique')
    op.drop_constraint('rel_mbn_rpt_gbd_brt_ligt_in_gebieden_buurt__tid_key', 'rel_mbn_rpt_gbd_brt_ligt_in_gebieden_buurt', type_='unique')
    op.create_unique_constraint('rel_mbn_rpt_gbd_brt_ligt_in_buurt_uniq', 'rel_mbn_rpt_gbd_brt_ligt_in_gebieden_buurt', ['_source_id'])
    op.create_unique_constraint('rel_mbn_rpt_gbd_brt_ligt_in_buurt__tid_key', 'rel_mbn_rpt_gbd_brt_ligt_in_gebieden_buurt', ['_tid'])
    op.drop_constraint('rel_mbn_rpt_gbd_bbk_ligt_in_gebieden_bouwblok_uniq', 'rel_mbn_rpt_gbd_bbk_ligt_in_gebieden_bouwblok', type_='unique')
    op.drop_constraint('rel_mbn_rpt_gbd_bbk_ligt_in_gebieden_bouwblok__tid_key', 'rel_mbn_rpt_gbd_bbk_ligt_in_gebieden_bouwblok', type_='unique')
    op.create_unique_constraint('rel_mbn_rpt_gbd_bbk_ligt_in_bouwblok_uniq', 'rel_mbn_rpt_gbd_bbk_ligt_in_gebieden_bouwblok', ['_source_id'])
    op.create_unique_constraint('rel_mbn_rpt_gbd_bbk_ligt_in_bouwblok__tid_key', 'rel_mbn_rpt_gbd_bbk_ligt_in_gebieden_bouwblok', ['_tid'])
    op.drop_constraint('rel_mbn_rlg_gbd_bbk_is_gemeten_van_gebieden_bouwblok_uniq', 'rel_mbn_rlg_gbd_bbk_is_gemeten_van_gebieden_bouwblok', type_='unique')
    op.drop_constraint('rel_mbn_rlg_gbd_bbk_is_gemeten_van_gebieden_bouwblok__tid_key', 'rel_mbn_rlg_gbd_bbk_is_gemeten_van_gebieden_bouwblok', type_='unique')
    op.create_unique_constraint('rel_mbn_rlg_gbd_bbk_is_gemeten_van_bouwblok_uniq', 'rel_mbn_rlg_gbd_bbk_is_gemeten_van_gebieden_bouwblok', ['_source_id'])
    op.create_unique_constraint('rel_mbn_rlg_gbd_bbk_is_gemeten_van_bouwblok__tid_key', 'rel_mbn_rlg_gbd_bbk_is_gemeten_van_gebieden_bouwblok', ['_tid'])
    op.drop_constraint('rel_mbn_mtg_mbn_mbt_hoort_bij_meetbouten_meetbout_uniq', 'rel_mbn_mtg_mbn_mbt_hoort_bij_meetbouten_meetbout', type_='unique')
    op.drop_constraint('rel_mbn_mtg_mbn_mbt_hoort_bij_meetbouten_meetbout__tid_key', 'rel_mbn_mtg_mbn_mbt_hoort_bij_meetbouten_meetbout', type_='unique')
    op.create_unique_constraint('rel_mbn_mtg_mbn_mbt_hoort_bij_meetbout_uniq', 'rel_mbn_mtg_mbn_mbt_hoort_bij_meetbouten_meetbout', ['_source_id'])
    op.create_unique_constraint('rel_mbn_mtg_mbn_mbt_hoort_bij_meetbout__tid_key', 'rel_mbn_mtg_mbn_mbt_hoort_bij_meetbouten_meetbout', ['_tid'])
    op.drop_constraint('rel_mbn_mbt_gbd_sdl_ligt_in_gebieden_stadsdeel_uniq', 'rel_mbn_mbt_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.drop_constraint('rel_mbn_mbt_gbd_sdl_ligt_in_gebieden_stadsdeel__tid_key', 'rel_mbn_mbt_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.create_unique_constraint('rel_mbn_mbt_gbd_sdl_ligt_in_stadsdeel_uniq', 'rel_mbn_mbt_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_source_id'])
    op.create_unique_constraint('rel_mbn_mbt_gbd_sdl_ligt_in_stadsdeel__tid_key', 'rel_mbn_mbt_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_tid'])
    op.drop_constraint('rel_mbn_mbt_gbd_brt_ligt_in_gebieden_buurt_uniq', 'rel_mbn_mbt_gbd_brt_ligt_in_gebieden_buurt', type_='unique')
    op.drop_constraint('rel_mbn_mbt_gbd_brt_ligt_in_gebieden_buurt__tid_key', 'rel_mbn_mbt_gbd_brt_ligt_in_gebieden_buurt', type_='unique')
    op.create_unique_constraint('rel_mbn_mbt_gbd_brt_ligt_in_buurt_uniq', 'rel_mbn_mbt_gbd_brt_ligt_in_gebieden_buurt', ['_source_id'])
    op.create_unique_constraint('rel_mbn_mbt_gbd_brt_ligt_in_buurt__tid_key', 'rel_mbn_mbt_gbd_brt_ligt_in_gebieden_buurt', ['_tid'])
    op.drop_constraint('rel_mbn_mbt_gbd_bbk_ligt_in_gebieden_bouwblok_uniq', 'rel_mbn_mbt_gbd_bbk_ligt_in_gebieden_bouwblok', type_='unique')
    op.drop_constraint('rel_mbn_mbt_gbd_bbk_ligt_in_gebieden_bouwblok__tid_key', 'rel_mbn_mbt_gbd_bbk_ligt_in_gebieden_bouwblok', type_='unique')
    op.create_unique_constraint('rel_mbn_mbt_gbd_bbk_ligt_in_bouwblok_uniq', 'rel_mbn_mbt_gbd_bbk_ligt_in_gebieden_bouwblok', ['_source_id'])
    op.create_unique_constraint('rel_mbn_mbt_gbd_bbk_ligt_in_bouwblok__tid_key', 'rel_mbn_mbt_gbd_bbk_ligt_in_gebieden_bouwblok', ['_tid'])
    op.drop_constraint('rel_gbd_wijk_gbd_sdl_ligt_in_gebieden_stadsdeel_uniq', 'rel_gbd_wijk_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.drop_constraint('rel_gbd_wijk_gbd_sdl_ligt_in_gebieden_stadsdeel__tid_key', 'rel_gbd_wijk_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.create_unique_constraint('rel_gbd_wijk_gbd_sdl_ligt_in_stadsdeel_uniq', 'rel_gbd_wijk_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_source_id'])
    op.create_unique_constraint('rel_gbd_wijk_gbd_sdl_ligt_in_stadsdeel__tid_key', 'rel_gbd_wijk_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_tid'])
    op.drop_constraint('rel_gbd_wijk_gbd_ggw_ligt_in_gebieden_ggwgebied_uniq', 'rel_gbd_wijk_gbd_ggw_ligt_in_gebieden_ggwgebied', type_='unique')
    op.drop_constraint('rel_gbd_wijk_gbd_ggw_ligt_in_gebieden_ggwgebied__tid_key', 'rel_gbd_wijk_gbd_ggw_ligt_in_gebieden_ggwgebied', type_='unique')
    op.create_unique_constraint('rel_gbd_wijk_gbd_ggw_ligt_in_ggwgebied_uniq', 'rel_gbd_wijk_gbd_ggw_ligt_in_gebieden_ggwgebied', ['_source_id'])
    op.create_unique_constraint('rel_gbd_wijk_gbd_ggw_ligt_in_ggwgebied__tid_key', 'rel_gbd_wijk_gbd_ggw_ligt_in_gebieden_ggwgebied', ['_tid'])
    op.drop_constraint('rel_gbd_wijk_brk_gme_ligt_in_brk_gemeente_uniq', 'rel_gbd_wijk_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.drop_constraint('rel_gbd_wijk_brk_gme_ligt_in_brk_gemeente__tid_key', 'rel_gbd_wijk_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.create_unique_constraint('rel_gbd_wijk_brk_gme_ligt_in_gemeente_uniq', 'rel_gbd_wijk_brk_gme_ligt_in_brk_gemeente', ['_source_id'])
    op.create_unique_constraint('rel_gbd_wijk_brk_gme_ligt_in_gemeente__tid_key', 'rel_gbd_wijk_brk_gme_ligt_in_brk_gemeente', ['_tid'])
    op.drop_constraint('rel_gbd_sdl_brk_gme_ligt_in_brk_gemeente_uniq', 'rel_gbd_sdl_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.drop_constraint('rel_gbd_sdl_brk_gme_ligt_in_brk_gemeente__tid_key', 'rel_gbd_sdl_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.create_unique_constraint('rel_gbd_sdl_brk_gme_ligt_in_gemeente_uniq', 'rel_gbd_sdl_brk_gme_ligt_in_brk_gemeente', ['_source_id'])
    op.create_unique_constraint('rel_gbd_sdl_brk_gme_ligt_in_gemeente__tid_key', 'rel_gbd_sdl_brk_gme_ligt_in_brk_gemeente', ['_tid'])
    op.drop_constraint('rel_gbd_ggw_gbd_sdl_ligt_in_gebieden_stadsdeel_uniq', 'rel_gbd_ggw_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.drop_constraint('rel_gbd_ggw_gbd_sdl_ligt_in_gebieden_stadsdeel__tid_key', 'rel_gbd_ggw_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.create_unique_constraint('rel_gbd_ggw_gbd_sdl_ligt_in_stadsdeel_uniq', 'rel_gbd_ggw_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_source_id'])
    op.create_unique_constraint('rel_gbd_ggw_gbd_sdl_ligt_in_stadsdeel__tid_key', 'rel_gbd_ggw_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_tid'])
    op.drop_constraint('rel_gbd_ggw_gbd_brt_bestaat_uit_gebieden_buurten_uniq', 'rel_gbd_ggw_gbd_brt_bestaat_uit_gebieden_buurten', type_='unique')
    op.drop_constraint('rel_gbd_ggw_gbd_brt_bestaat_uit_gebieden_buurten__tid_key', 'rel_gbd_ggw_gbd_brt_bestaat_uit_gebieden_buurten', type_='unique')
    op.create_unique_constraint('rel_gbd_ggw_gbd_brt_bestaat_uit_buurten_uniq', 'rel_gbd_ggw_gbd_brt_bestaat_uit_gebieden_buurten', ['_source_id'])
    op.create_unique_constraint('rel_gbd_ggw_gbd_brt_bestaat_uit_buurten__tid_key', 'rel_gbd_ggw_gbd_brt_bestaat_uit_gebieden_buurten', ['_tid'])
    op.drop_constraint('rel_gbd_ggw_brk_gme_ligt_in_brk_gemeente_uniq', 'rel_gbd_ggw_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.drop_constraint('rel_gbd_ggw_brk_gme_ligt_in_brk_gemeente__tid_key', 'rel_gbd_ggw_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.create_unique_constraint('rel_gbd_ggw_brk_gme_ligt_in_gemeente_uniq', 'rel_gbd_ggw_brk_gme_ligt_in_brk_gemeente', ['_source_id'])
    op.create_unique_constraint('rel_gbd_ggw_brk_gme_ligt_in_gemeente__tid_key', 'rel_gbd_ggw_brk_gme_ligt_in_brk_gemeente', ['_tid'])
    op.drop_constraint('rel_gbd_ggp_gbd_sdl_ligt_in_gebieden_stadsdeel_uniq', 'rel_gbd_ggp_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.drop_constraint('rel_gbd_ggp_gbd_sdl_ligt_in_gebieden_stadsdeel__tid_key', 'rel_gbd_ggp_gbd_sdl_ligt_in_gebieden_stadsdeel', type_='unique')
    op.create_unique_constraint('rel_gbd_ggp_gbd_sdl_ligt_in_stadsdeel_uniq', 'rel_gbd_ggp_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_source_id'])
    op.create_unique_constraint('rel_gbd_ggp_gbd_sdl_ligt_in_stadsdeel__tid_key', 'rel_gbd_ggp_gbd_sdl_ligt_in_gebieden_stadsdeel', ['_tid'])
    op.drop_constraint('rel_gbd_ggp_gbd_brt_bestaat_uit_gebieden_buurten_uniq', 'rel_gbd_ggp_gbd_brt_bestaat_uit_gebieden_buurten', type_='unique')
    op.drop_constraint('rel_gbd_ggp_gbd_brt_bestaat_uit_gebieden_buurten__tid_key', 'rel_gbd_ggp_gbd_brt_bestaat_uit_gebieden_buurten', type_='unique')
    op.create_unique_constraint('rel_gbd_ggp_gbd_brt_bestaat_uit_buurten_uniq', 'rel_gbd_ggp_gbd_brt_bestaat_uit_gebieden_buurten', ['_source_id'])
    op.create_unique_constraint('rel_gbd_ggp_gbd_brt_bestaat_uit_buurten__tid_key', 'rel_gbd_ggp_gbd_brt_bestaat_uit_gebieden_buurten', ['_tid'])
    op.drop_constraint('rel_gbd_ggp_brk_gme_ligt_in_brk_gemeente_uniq', 'rel_gbd_ggp_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.drop_constraint('rel_gbd_ggp_brk_gme_ligt_in_brk_gemeente__tid_key', 'rel_gbd_ggp_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.create_unique_constraint('rel_gbd_ggp_brk_gme_ligt_in_gemeente_uniq', 'rel_gbd_ggp_brk_gme_ligt_in_brk_gemeente', ['_source_id'])
    op.create_unique_constraint('rel_gbd_ggp_brk_gme_ligt_in_gemeente__tid_key', 'rel_gbd_ggp_brk_gme_ligt_in_brk_gemeente', ['_tid'])
    op.drop_constraint('rel_gbd_brt_gbd_wijk_ligt_in_gebieden_wijk_uniq', 'rel_gbd_brt_gbd_wijk_ligt_in_gebieden_wijk', type_='unique')
    op.drop_constraint('rel_gbd_brt_gbd_wijk_ligt_in_gebieden_wijk__tid_key', 'rel_gbd_brt_gbd_wijk_ligt_in_gebieden_wijk', type_='unique')
    op.create_unique_constraint('rel_gbd_brt_gbd_wijk_ligt_in_wijk_uniq', 'rel_gbd_brt_gbd_wijk_ligt_in_gebieden_wijk', ['_source_id'])
    op.create_unique_constraint('rel_gbd_brt_gbd_wijk_ligt_in_wijk__tid_key', 'rel_gbd_brt_gbd_wijk_ligt_in_gebieden_wijk', ['_tid'])
    op.drop_constraint('rel_gbd_brt_gbd_ggw_ligt_in_gebieden_ggwgebied_uniq', 'rel_gbd_brt_gbd_ggw_ligt_in_gebieden_ggwgebied', type_='unique')
    op.drop_constraint('rel_gbd_brt_gbd_ggw_ligt_in_gebieden_ggwgebied__tid_key', 'rel_gbd_brt_gbd_ggw_ligt_in_gebieden_ggwgebied', type_='unique')
    op.create_unique_constraint('rel_gbd_brt_gbd_ggw_ligt_in_ggwgebied_uniq', 'rel_gbd_brt_gbd_ggw_ligt_in_gebieden_ggwgebied', ['_source_id'])
    op.create_unique_constraint('rel_gbd_brt_gbd_ggw_ligt_in_ggwgebied__tid_key', 'rel_gbd_brt_gbd_ggw_ligt_in_gebieden_ggwgebied', ['_tid'])
    op.drop_constraint('rel_gbd_brt_gbd_ggp_ligt_in_gebieden_ggpgebied_uniq', 'rel_gbd_brt_gbd_ggp_ligt_in_gebieden_ggpgebied', type_='unique')
    op.drop_constraint('rel_gbd_brt_gbd_ggp_ligt_in_gebieden_ggpgebied__tid_key', 'rel_gbd_brt_gbd_ggp_ligt_in_gebieden_ggpgebied', type_='unique')
    op.create_unique_constraint('rel_gbd_brt_gbd_ggp_ligt_in_ggpgebied_uniq', 'rel_gbd_brt_gbd_ggp_ligt_in_gebieden_ggpgebied', ['_source_id'])
    op.create_unique_constraint('rel_gbd_brt_gbd_ggp_ligt_in_ggpgebied__tid_key', 'rel_gbd_brt_gbd_ggp_ligt_in_gebieden_ggpgebied', ['_tid'])
    op.drop_constraint('rel_gbd_brt_brk_gme_ligt_in_brk_gemeente_uniq', 'rel_gbd_brt_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.drop_constraint('rel_gbd_brt_brk_gme_ligt_in_brk_gemeente__tid_key', 'rel_gbd_brt_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.create_unique_constraint('rel_gbd_brt_brk_gme_ligt_in_gemeente_uniq', 'rel_gbd_brt_brk_gme_ligt_in_brk_gemeente', ['_source_id'])
    op.create_unique_constraint('rel_gbd_brt_brk_gme_ligt_in_gemeente__tid_key', 'rel_gbd_brt_brk_gme_ligt_in_brk_gemeente', ['_tid'])
    op.drop_constraint('rel_gbd_bbk_gbd_brt_ligt_in_gebieden_buurt_uniq', 'rel_gbd_bbk_gbd_brt_ligt_in_gebieden_buurt', type_='unique')
    op.drop_constraint('rel_gbd_bbk_gbd_brt_ligt_in_gebieden_buurt__tid_key', 'rel_gbd_bbk_gbd_brt_ligt_in_gebieden_buurt', type_='unique')
    op.create_unique_constraint('rel_gbd_bbk_gbd_brt_ligt_in_buurt_uniq', 'rel_gbd_bbk_gbd_brt_ligt_in_gebieden_buurt', ['_source_id'])
    op.create_unique_constraint('rel_gbd_bbk_gbd_brt_ligt_in_buurt__tid_key', 'rel_gbd_bbk_gbd_brt_ligt_in_gebieden_buurt', ['_tid'])
    op.drop_constraint('rel_gbd_bbk_brk_gme_ligt_in_brk_gemeente_uniq', 'rel_gbd_bbk_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.drop_constraint('rel_gbd_bbk_brk_gme_ligt_in_brk_gemeente__tid_key', 'rel_gbd_bbk_brk_gme_ligt_in_brk_gemeente', type_='unique')
    op.create_unique_constraint('rel_gbd_bbk_brk_gme_ligt_in_gemeente_uniq', 'rel_gbd_bbk_brk_gme_ligt_in_brk_gemeente', ['_source_id'])
    op.create_unique_constraint('rel_gbd_bbk_brk_gme_ligt_in_gemeente__tid_key', 'rel_gbd_bbk_brk_gme_ligt_in_brk_gemeente', ['_tid'])
    op.create_table('rel_mbn_rpt_bag_nag_nabij_nummeraanduiding',
    sa.Column('_gobid', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('_id', sa.VARCHAR(), autoincrement=False, nullable=True),
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
    sa.Column('id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('src_source', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('src_id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('src_volgnummer', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('derivation', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('dst_source', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('dst_id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('dst_volgnummer', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('_expiration_date', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('bronwaarde', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('begin_geldigheid', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('eind_geldigheid', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_last_dst_event', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('_last_src_event', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('_tid', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.ForeignKeyConstraint(['dst_id', 'dst_volgnummer'], ['bag_nummeraanduidingen._id', 'bag_nummeraanduidingen.volgnummer'], name='rel_mbn_rpt_bag_nag_nabij_nummeraanduiding_dfk'),
    sa.ForeignKeyConstraint(['src_id'], ['meetbouten_referentiepunten._id'], name='rel_mbn_rpt_bag_nag_nabij_nummeraanduiding_sfk'),
    sa.PrimaryKeyConstraint('_gobid', name='rel_mbn_rpt_bag_nag_nabij_nummeraanduiding_pkey'),
    sa.UniqueConstraint('_source_id', name='rel_mbn_rpt_bag_nag_nabij_nummeraanduiding_uniq'),
    sa.UniqueConstraint('_tid', name='rel_mbn_rpt_bag_nag_nabij_nummeraanduiding__tid_key')
    )
    op.create_table('rel_mbn_mtg_mbn_rpt__rft_n__meetbouten_referentiepunten',
    sa.Column('_gobid', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('_id', sa.VARCHAR(), autoincrement=False, nullable=True),
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
    sa.Column('id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('src_source', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('src_id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('src_volgnummer', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('derivation', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('dst_source', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('dst_id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('dst_volgnummer', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('_expiration_date', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('bronwaarde', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('begin_geldigheid', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('eind_geldigheid', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_last_dst_event', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('_last_src_event', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('_tid', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.ForeignKeyConstraint(['dst_id'], ['meetbouten_referentiepunten._id'], name='rel_mbn_mtg_mbn_rpt_refereert_aan_referentiepunten_dfk'),
    sa.ForeignKeyConstraint(['src_id'], ['meetbouten_metingen._id'], name='rel_mbn_mtg_mbn_rpt_refereert_aan_referentiepunten_sfk'),
    sa.PrimaryKeyConstraint('_gobid', name='rel_mbn_mtg_mbn_rpt_refereert_aan_referentiepunten_pkey'),
    sa.UniqueConstraint('_source_id', name='rel_mbn_mtg_mbn_rpt_refereert_aan_referentiepunten_uniq'),
    sa.UniqueConstraint('_tid', name='rel_mbn_mtg_mbn_rpt_refereert_aan_referentiepunten__tid_key')
    )
    op.create_table('rel_mbn_mbt_bag_nag_nabij_nummeraanduiding',
    sa.Column('_gobid', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('_id', sa.VARCHAR(), autoincrement=False, nullable=True),
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
    sa.Column('id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('src_source', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('src_id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('src_volgnummer', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('derivation', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('dst_source', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('dst_id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('dst_volgnummer', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('_expiration_date', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('bronwaarde', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('begin_geldigheid', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('eind_geldigheid', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_last_dst_event', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('_last_src_event', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('_tid', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.ForeignKeyConstraint(['dst_id', 'dst_volgnummer'], ['bag_nummeraanduidingen._id', 'bag_nummeraanduidingen.volgnummer'], name='rel_mbn_mbt_bag_nag_nabij_nummeraanduiding_dfk'),
    sa.ForeignKeyConstraint(['src_id'], ['meetbouten_meetbouten._id'], name='rel_mbn_mbt_bag_nag_nabij_nummeraanduiding_sfk'),
    sa.PrimaryKeyConstraint('_gobid', name='rel_mbn_mbt_bag_nag_nabij_nummeraanduiding_pkey'),
    sa.UniqueConstraint('_source_id', name='rel_mbn_mbt_bag_nag_nabij_nummeraanduiding_uniq'),
    sa.UniqueConstraint('_tid', name='rel_mbn_mbt_bag_nag_nabij_nummeraanduiding__tid_key')
    )
    op.drop_table('rel_mbn_mtg_mbn_rpt__meetbouten_refpnt_rft_aan_')
    # ### end Alembic commands ###
