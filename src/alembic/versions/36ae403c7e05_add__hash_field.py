"""Add _hash field

Revision ID: 36ae403c7e05
Revises: a42e22d67afd
Create Date: 2019-02-05 15:19:04.007808

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '36ae403c7e05'
down_revision = 'a42e22d67afd'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    # Drop views to allow columns to be changed, GOB-Upload will recreate them
    op.execute('DROP VIEW IF EXISTS meetbouten_meetbouten_enhanced')
    op.execute('DROP VIEW IF EXISTS meetbouten_metingen_enhanced')
    op.execute('DROP VIEW IF EXISTS meetbouten_referentiepunten_enhanced')
    op.execute('DROP VIEW IF EXISTS meetbouten_rollagen_enhanced')
    op.execute('DROP VIEW IF EXISTS nap_peilmerken_enhanced')
    op.execute('DROP VIEW IF EXISTS gebieden_bouwblokken_enhanced')
    op.execute('DROP VIEW IF EXISTS gebieden_buurten_enhanced')
    op.execute('DROP VIEW IF EXISTS gebieden_ggpgebieden_enhanced')
    op.execute('DROP VIEW IF EXISTS gebieden_ggwgebieden_enhanced')
    op.execute('DROP VIEW IF EXISTS gebieden_wijken_enhanced')
    op.execute('DROP VIEW IF EXISTS gebieden_stadsdelen_enhanced')

    op.add_column('bag_ligplaatsen', sa.Column('_hash', sa.String(), autoincrement=False, nullable=True))
    op.add_column('bag_openbareruimtes', sa.Column('_hash', sa.String(), autoincrement=False, nullable=True))
    op.add_column('bag_standplaatsen', sa.Column('_hash', sa.String(), autoincrement=False, nullable=True))
    op.add_column('bag_woonplaatsen', sa.Column('_hash', sa.String(), autoincrement=False, nullable=True))
    op.add_column('gebieden_bouwblokken', sa.Column('_hash', sa.String(), autoincrement=False, nullable=True))
    op.add_column('gebieden_buurten', sa.Column('_hash', sa.String(), autoincrement=False, nullable=True))
    op.add_column('gebieden_ggpgebieden', sa.Column('_hash', sa.String(), autoincrement=False, nullable=True))
    op.add_column('gebieden_ggwgebieden', sa.Column('_hash', sa.String(), autoincrement=False, nullable=True))
    op.add_column('gebieden_stadsdelen', sa.Column('_hash', sa.String(), autoincrement=False, nullable=True))
    op.add_column('gebieden_wijken', sa.Column('_hash', sa.String(), autoincrement=False, nullable=True))
    op.add_column('meetbouten_meetbouten', sa.Column('_hash', sa.String(), autoincrement=False, nullable=True))
    op.add_column('meetbouten_metingen', sa.Column('_hash', sa.String(), autoincrement=False, nullable=True))
    op.add_column('meetbouten_referentiepunten', sa.Column('_hash', sa.String(), autoincrement=False, nullable=True))
    op.add_column('meetbouten_rollagen', sa.Column('_hash', sa.String(), autoincrement=False, nullable=True))
    op.add_column('nap_peilmerken', sa.Column('_hash', sa.String(), autoincrement=False, nullable=True))
    op.add_column('test_catalogue_test_entity', sa.Column('_hash', sa.String(), autoincrement=False, nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    # Drop views to allow columns to be changed, GOB-Upload will recreate them
    op.execute('DROP VIEW IF EXISTS meetbouten_meetbouten_enhanced')
    op.execute('DROP VIEW IF EXISTS meetbouten_metingen_enhanced')
    op.execute('DROP VIEW IF EXISTS meetbouten_referentiepunten_enhanced')
    op.execute('DROP VIEW IF EXISTS meetbouten_rollagen_enhanced')
    op.execute('DROP VIEW IF EXISTS nap_peilmerken_enhanced')
    op.execute('DROP VIEW IF EXISTS gebieden_bouwblokken_enhanced')
    op.execute('DROP VIEW IF EXISTS gebieden_buurten_enhanced')
    op.execute('DROP VIEW IF EXISTS gebieden_ggpgebieden_enhanced')
    op.execute('DROP VIEW IF EXISTS gebieden_ggwgebieden_enhanced')
    op.execute('DROP VIEW IF EXISTS gebieden_wijken_enhanced')
    op.execute('DROP VIEW IF EXISTS gebieden_stadsdelen_enhanced')

    op.drop_column('test_catalogue_test_entity', '_hash')
    op.drop_column('nap_peilmerken', '_hash')
    op.drop_column('meetbouten_rollagen', '_hash')
    op.drop_column('meetbouten_referentiepunten', '_hash')
    op.drop_column('meetbouten_metingen', '_hash')
    op.drop_column('meetbouten_meetbouten', '_hash')
    op.drop_column('gebieden_wijken', '_hash')
    op.drop_column('gebieden_stadsdelen', '_hash')
    op.drop_column('gebieden_ggwgebieden', '_hash')
    op.drop_column('gebieden_ggpgebieden', '_hash')
    op.drop_column('gebieden_buurten', '_hash')
    op.drop_column('gebieden_bouwblokken', '_hash')
    op.drop_column('bag_woonplaatsen', '_hash')
    op.drop_column('bag_standplaatsen', '_hash')
    op.drop_column('bag_openbareruimtes', '_hash')
    op.drop_column('bag_ligplaatsen', '_hash')
    # ### end Alembic commands ###
