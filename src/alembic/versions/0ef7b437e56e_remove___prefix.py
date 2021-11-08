"""Remove _ prefix

Revision ID: 0ef7b437e56e
Revises: ede7f6856dff
Create Date: 2021-11-02 16:51:32.575717

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '0ef7b437e56e'
down_revision = 'ede7f6856dff'
branch_labels = None
depends_on = None


def upgrade():
    op.rename_table("rel_gbd_brt_gbd_ggp__ligt_in_ggpgebied", "rel_gbd_brt_gbd_ggp_ligt_in_ggpgebied")
    op.rename_table("rel_gbd_brt_gbd_ggw__ligt_in_ggwgebied", "rel_gbd_brt_gbd_ggw_ligt_in_ggwgebied")
    op.rename_table("rel_gbd_wijk_gbd_ggw__ligt_in_ggwgebied", "rel_gbd_wijk_gbd_ggw_ligt_in_ggwgebied")
    op.alter_column("gebieden_buurten", "_ligt_in_ggpgebied", new_column_name="ligt_in_ggpgebied")
    op.alter_column("gebieden_buurten", "_ligt_in_ggwgebied", new_column_name="ligt_in_ggwgebied")
    op.alter_column("gebieden_wijken", "_ligt_in_ggwgebied", new_column_name="ligt_in_ggwgebied")


def downgrade():
    op.rename_table("rel_gbd_brt_gbd_ggp_ligt_in_ggpgebied", "rel_gbd_brt_gbd_ggp__ligt_in_ggpgebied")
    op.rename_table("rel_gbd_brt_gbd_ggw_ligt_in_ggwgebied", "rel_gbd_brt_gbd_ggw__ligt_in_ggwgebied")
    op.rename_table("rel_gbd_wijk_gbd_ggw_ligt_in_ggwgebied", "rel_gbd_wijk_gbd_ggw__ligt_in_ggwgebied")
    op.alter_column("gebieden_buurten", "ligt_in_ggpgebied", new_column_name="_ligt_in_ggpgebied")
    op.alter_column("gebieden_buurten", "ligt_in_ggwgebied", new_column_name="_ligt_in_ggwgebied")
    op.alter_column("gebieden_wijken", "ligt_in_ggwgebied", new_column_name="_ligt_in_ggwgebied")
