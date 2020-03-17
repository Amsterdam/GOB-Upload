"""shorten last table names

Revision ID: f876506602dd
Revises: 28b4ae9aa0fb
Create Date: 2020-03-17 08:40:42.267353

"""
from alembic import op
import re
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'f876506602dd'
down_revision = '28b4ae9aa0fb'
branch_labels = None
depends_on = None

renames = [
    ('rel_brk_kot_brk_kce_aangeduid_door_kadastralegemeentecode',
     'rel_brk_kot_brk_kce__angdd_dr__kadastralegemeentecode'),
    ('rel_brk_kse_brk_kce_is_onderdeel_van_kadastralegemeentecode',
     'rel_brk_kse_brk_kce__ondrdl_vn__kadastralegemeentecode'),
    ('rel_brk_kot_brk_kge_aangeduid_door_kadastralegemeente', 'rel_brk_kot_brk_kge__angdd_dr__kadastralegemeente'),
    ('rel_brk_kce_brk_kge_is_onderdeel_van_kadastralegemeente', 'rel_brk_kce_brk_kge__ondrdl_vn__kadastralegemeente'),
    ('rel_brk_kot_brk_kse_aangeduid_door_kadastralesectie', 'rel_brk_kot_brk_kse__angdd_dr__kadastralesectie'),
    ('rel_brk_kot_brk_gme_aangeduid_door_gemeente', 'rel_brk_kot_brk_gme__angdd_dr__gemeente'),
    ('rel_brk_akt_brk_kot_heeft_betrekking_op_kadastraal_object', 'rel_brk_akt_brk_kot__hft_btrk_p__kadastraal_object'),
    ('rel_brk_kot_bag_vot_heeft_een_relatie_met_verblijfsobject', 'rel_brk_kot_bag_vot__hft_rel_mt__verblijfsobject'),
]


def mv_name(tablename):
    return re.sub(r'^rel_(.*)', 'mv_\g<1>', tablename)


def upgrade():

    for (old, new) in renames:
        op.execute(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name(old)} CASCADE")
        op.execute(f"ALTER TABLE {old} RENAME TO {new}")


def downgrade():

    for (old, new) in renames:
        op.execute(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name(new)} CASCADE")
        op.execute(f"ALTER TABLE {new} RENAME TO {old}")
