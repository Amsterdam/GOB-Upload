"""Meetbouten to Amschema

Revision ID: cda2005bcf0e
Revises: 5ab4ad3a4430
Create Date: 2022-09-21 10:59:55.190765

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
import geoalchemy2


# revision identifiers, used by Alembic.
revision = 'cda2005bcf0e'
down_revision = '5ab4ad3a4430'
branch_labels = None
depends_on = None


# This file is auto-generated with GOBModel2AmsterdamSchema
#
# To use this migration, generated a new empty migration with alembic:
# alembic revision -m "Your revision message"
# Open the newly generated revision file, and replace the existing (empty) upgrade() en downgrade() functions with the
# contents of this file.

from gobupload.alembic_utils import (
    get_query_split_json_column,
    get_query_merge_columns_to_jsonb_column,
    RenamedRelation,
    upgrade_relations,
    downgrade_relations
)

renamed_relations = [
    RenamedRelation(
        table_name="meetbouten_meetbouten",
        old_column="ligt_in_bouwblok",
        new_column="ligt_in_gebieden_bouwblok",
        old_relation_table="rel_mbn_mbt_gbd_bbk_ligt_in_bouwblok",
        new_relation_table="rel_mbn_mbt_gbd_bbk_ligt_in_gebieden_bouwblok"
    ),
    RenamedRelation(
        table_name="meetbouten_meetbouten",
        old_column="ligt_in_buurt",
        new_column="ligt_in_gebieden_buurt",
        old_relation_table="rel_mbn_mbt_gbd_brt_ligt_in_buurt",
        new_relation_table="rel_mbn_mbt_gbd_brt_ligt_in_gebieden_buurt"
    ),
    RenamedRelation(
        table_name="meetbouten_meetbouten",
        old_column="ligt_in_stadsdeel",
        new_column="ligt_in_gebieden_stadsdeel",
        old_relation_table="rel_mbn_mbt_gbd_sdl_ligt_in_stadsdeel",
        new_relation_table="rel_mbn_mbt_gbd_sdl_ligt_in_gebieden_stadsdeel"
    ),
    RenamedRelation(
        table_name="meetbouten_metingen",
        old_column="hoort_bij_meetbout",
        new_column="hoort_bij_meetbouten_meetbout",
        old_relation_table="rel_mbn_mtg_mbn_mbt_hoort_bij_meetbout",
        new_relation_table="rel_mbn_mtg_mbn_mbt_hoort_bij_meetbouten_meetbout"
    ),
    RenamedRelation(
        table_name="meetbouten_metingen",
        old_column="refereert_aan_referentiepunten",
        new_column="refereert_aan_meetbouten_referentiepunten",
        old_relation_table="rel_mbn_mtg_mbn_rpt_refereert_aan_referentiepunten",
        new_relation_table="rel_mbn_mtg_mbn_rpt__rft_n__meetbouten_referentiepunten"
    ),
    RenamedRelation(
        table_name="meetbouten_referentiepunten",
        old_column="ligt_in_bouwblok",
        new_column="ligt_in_gebieden_bouwblok",
        old_relation_table="rel_mbn_rpt_gbd_bbk_ligt_in_bouwblok",
        new_relation_table="rel_mbn_rpt_gbd_bbk_ligt_in_gebieden_bouwblok"
    ),
    RenamedRelation(
        table_name="meetbouten_referentiepunten",
        old_column="ligt_in_buurt",
        new_column="ligt_in_gebieden_buurt",
        old_relation_table="rel_mbn_rpt_gbd_brt_ligt_in_buurt",
        new_relation_table="rel_mbn_rpt_gbd_brt_ligt_in_gebieden_buurt"
    ),
    RenamedRelation(
        table_name="meetbouten_referentiepunten",
        old_column="ligt_in_stadsdeel",
        new_column="ligt_in_gebieden_stadsdeel",
        old_relation_table="rel_mbn_rpt_gbd_sdl_ligt_in_stadsdeel",
        new_relation_table="rel_mbn_rpt_gbd_sdl_ligt_in_gebieden_stadsdeel"
    ),
    RenamedRelation(
        table_name="meetbouten_rollagen",
        old_column="is_gemeten_van_bouwblok",
        new_column="is_gemeten_van_gebieden_bouwblok",
        old_relation_table="rel_mbn_rlg_gbd_bbk_is_gemeten_van_bouwblok",
        new_relation_table="rel_mbn_rlg_gbd_bbk_is_gemeten_van_gebieden_bouwblok"
    ),
]


def upgrade():
    op.add_column('meetbouten_meetbouten', sa.Column('datum_actueel_tot', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('meetbouten_metingen', sa.Column('datum_actueel_tot', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('meetbouten_referentiepunten', sa.Column('datum_actueel_tot', sa.DateTime(), autoincrement=False, nullable=True))
    op.add_column('meetbouten_rollagen', sa.Column('datum_actueel_tot', sa.DateTime(), autoincrement=False, nullable=True))

    # Split json column nabij_nummeraanduiding into separate columns
    op.add_column('meetbouten_meetbouten', sa.Column('nabij_adres', sa.String(), autoincrement=False, nullable=True))
    op.execute(get_query_split_json_column('meetbouten_meetbouten', 'nabij_nummeraanduiding', {
        'bronwaarde': 'nabij_adres',
    }, {
        'bronwaarde': 'varchar',
    }))
    op.execute("ALTER TABLE meetbouten_meetbouten DROP COLUMN nabij_nummeraanduiding CASCADE")

    # Split json column nabij_nummeraanduiding into separate columns
    op.add_column('meetbouten_referentiepunten', sa.Column('nabij_adres', sa.String(), autoincrement=False, nullable=True))
    op.execute(get_query_split_json_column('meetbouten_referentiepunten', 'nabij_nummeraanduiding', {
        'bronwaarde': 'nabij_adres',
    }, {
        'bronwaarde': 'varchar',
    }))
    op.execute("ALTER TABLE meetbouten_referentiepunten DROP COLUMN nabij_nummeraanduiding CASCADE")

    upgrade_relations(op, renamed_relations)


def downgrade():
    downgrade_relations(op, renamed_relations)

    # Unsplit json column nabij_nummeraanduiding from separate columns
    op.add_column('meetbouten_meetbouten', sa.Column('nabij_nummeraanduiding', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    op.execute(get_query_merge_columns_to_jsonb_column('meetbouten_meetbouten', 'nabij_nummeraanduiding', {
        'bronwaarde': 'nabij_adres',
    }))
    op.drop_column('meetbouten_meetbouten', 'nabij_adres')

    # Unsplit json column nabij_nummeraanduiding from separate columns
    op.add_column('meetbouten_referentiepunten', sa.Column('nabij_nummeraanduiding', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    op.execute(get_query_merge_columns_to_jsonb_column('meetbouten_referentiepunten', 'nabij_nummeraanduiding', {
        'bronwaarde': 'nabij_adres',
    }))
    op.drop_column('meetbouten_referentiepunten', 'nabij_adres')


    op.drop_column('meetbouten_meetbouten', 'datum_actueel_tot')
    op.drop_column('meetbouten_metingen', 'datum_actueel_tot')
    op.drop_column('meetbouten_referentiepunten', 'datum_actueel_tot')
    op.drop_column('meetbouten_rollagen', 'datum_actueel_tot')
