"""Rename attributes to match specs

Revision ID: 9f515dab1ffb
Revises: 29149a285ff2
Create Date: 2019-01-17 12:53:12.962528

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '9f515dab1ffb'
down_revision = 'bc7e206f1c45'
branch_labels = None
depends_on = None


def upgrade():
    # Rename columns to match specs
    op.alter_column('meetbouten_metingen', 'refereert_aan', new_column_name='refereert_aan_referentiepunten')

    op.alter_column('gebieden_bouwblokken', 'datum_begin_geldigheid', new_column_name='begin_geldigheid')
    op.alter_column('gebieden_bouwblokken', 'datum_einde_geldigheid', new_column_name='eind_geldigheid')

    op.alter_column('gebieden_buurten', 'datum_begin_geldigheid', new_column_name='begin_geldigheid')
    op.alter_column('gebieden_buurten', 'datum_einde_geldigheid', new_column_name='eind_geldigheid')

    op.alter_column('gebieden_wijken', 'datum_begin_geldigheid', new_column_name='begin_geldigheid')
    op.alter_column('gebieden_wijken', 'datum_einde_geldigheid', new_column_name='eind_geldigheid')

    op.alter_column('gebieden_ggwgebieden', 'datum_begin_geldigheid', new_column_name='begin_geldigheid')
    op.alter_column('gebieden_ggwgebieden', 'datum_einde_geldigheid', new_column_name='eind_geldigheid')
    op.alter_column('gebieden_ggwgebieden', 'bestaat_uit_wijken', new_column_name='bestaat_uit_buurten')

    op.alter_column('gebieden_ggpgebieden', 'datum_begin_geldigheid', new_column_name='begin_geldigheid')
    op.alter_column('gebieden_ggpgebieden', 'datum_einde_geldigheid', new_column_name='eind_geldigheid')

    op.alter_column('gebieden_stadsdelen', 'datum_begin_geldigheid', new_column_name='begin_geldigheid')
    op.alter_column('gebieden_stadsdelen', 'datum_einde_geldigheid', new_column_name='eind_geldigheid')

    op.alter_column('bag_woonplaatsen', 'datum_begin_geldigheid', new_column_name='begin_geldigheid')
    op.alter_column('bag_woonplaatsen', 'datum_einde_geldigheid', new_column_name='eind_geldigheid')



def downgrade():
    # Rename columns
    op.alter_column('meetbouten_metingen', 'refereert_aan_referentiepunten', new_column_name='refereert_aan')

    op.alter_column('gebieden_bouwblokken', 'begin_geldigheid', new_column_name='datum_begin_geldigheid')
    op.alter_column('gebieden_bouwblokken', 'eind_geldigheid', new_column_name='datum_einde_geldigheid')

    op.alter_column('gebieden_buurten', 'begin_geldigheid', new_column_name='datum_begin_geldigheid')
    op.alter_column('gebieden_buurten', 'eind_geldigheid', new_column_name='datum_einde_geldigheid')

    op.alter_column('gebieden_wijken', 'begin_geldigheid', new_column_name='datum_begin_geldigheid')
    op.alter_column('gebieden_wijken', 'eind_geldigheid', new_column_name='datum_einde_geldigheid')

    op.alter_column('gebieden_ggwgebieden', 'begin_geldigheid', new_column_name='datum_begin_geldigheid')
    op.alter_column('gebieden_ggwgebieden', 'eind_geldigheid', new_column_name='datum_einde_geldigheid')
    op.alter_column('gebieden_ggwgebieden', 'bestaat_uit_buurten', new_column_name='bestaat_uit_wijken')

    op.alter_column('gebieden_ggpgebieden', 'begin_geldigheid', new_column_name='datum_begin_geldigheid')
    op.alter_column('gebieden_ggpgebieden', 'eind_geldigheid', new_column_name='datum_einde_geldigheid')

    op.alter_column('gebieden_stadsdelen', 'begin_geldigheid', new_column_name='datum_begin_geldigheid')
    op.alter_column('gebieden_stadsdelen', 'eind_geldigheid', new_column_name='datum_einde_geldigheid')

    op.alter_column('bag_woonplaatsen', 'begin_geldigheid', new_column_name='datum_begin_geldigheid')
    op.alter_column('bag_woonplaatsen', 'eind_geldigheid', new_column_name='datum_einde_geldigheid')
