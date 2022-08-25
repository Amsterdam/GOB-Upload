"""source gebieden wijken from amsterdam schema

Revision ID: 0778641b0580
Revises: 9fc091ab554d
Create Date: 2022-08-15 08:53:17.570625

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

from gobupload.alembic_utils import RenamedRelation, downgrade_relations, upgrade_relations

# revision identifiers, used by Alembic.
revision = '0778641b0580'
down_revision = '9fc091ab554d'
branch_labels = None
depends_on = None


renamed_relations = [
    RenamedRelation(
        table_name="gebieden_wijken",
        old_column="ligt_in_gemeente",
        new_column="ligt_in_brk_gemeente",
        old_relation_table="rel_gbd_wijk_brk_gme_ligt_in_gemeente",
        new_relation_table="rel_gbd_wijk_brk_gme_ligt_in_brk_gemeente"
    ),
    RenamedRelation(
        table_name="gebieden_wijken",
        old_column="ligt_in_ggwgebied",
        new_column="ligt_in_gebieden_ggwgebied",
        old_relation_table="rel_gbd_wijk_gbd_ggw_ligt_in_ggwgebied",
        new_relation_table="rel_gbd_wijk_gbd_ggw_ligt_in_gebieden_ggwgebied"
    ),
    RenamedRelation(
        table_name="gebieden_wijken",
        old_column="ligt_in_stadsdeel",
        new_column="ligt_in_gebieden_stadsdeel",
        old_relation_table="rel_gbd_wijk_gbd_sdl_ligt_in_stadsdeel",
        new_relation_table="rel_gbd_wijk_gbd_sdl_ligt_in_gebieden_stadsdeel"
    ),
]


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('gebieden_wijken', sa.Column('datum_actueel_tot', sa.DateTime(), autoincrement=False, nullable=True))

    upgrade_relations(op, renamed_relations)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('gebieden_wijken', 'datum_actueel_tot')

    downgrade_relations(op, renamed_relations)