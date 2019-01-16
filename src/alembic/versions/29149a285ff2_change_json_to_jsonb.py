"""Change JSON to JSONB

Revision ID: 29149a285ff2
Revises: 50a4daeb9e88
Create Date: 2019-01-16 10:45:25.691701

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '29149a285ff2'
down_revision = '50a4daeb9e88'
branch_labels = None
depends_on = None


def upgrade():
    # Drop views to allow columns to be changed, GOB-Upload will recreate them
    op.execute('drop view meetbouten_meetbouten_enhanced')
    op.execute('drop view meetbouten_metingen_enhanced')
    op.execute('drop view meetbouten_referentiepunten_enhanced')
    op.execute('drop view meetbouten_rollagen_enhanced')
    op.execute('drop view nap_peilmerken_enhanced')

    # Change all JSON columns to JSONB
    op.alter_column('meetbouten_meetbouten', 'nabij_nummeraanduiding', type_=postgresql.JSONB, postgresql_using='nabij_nummeraanduiding::text::jsonb')
    op.alter_column('meetbouten_meetbouten', 'status', type_=postgresql.JSONB, postgresql_using='status::text::jsonb')
    op.alter_column('meetbouten_meetbouten', 'merk', type_=postgresql.JSONB, postgresql_using='merk::text::jsonb')
    op.alter_column('meetbouten_meetbouten', 'ligt_in_bouwblok', type_=postgresql.JSONB, postgresql_using='ligt_in_bouwblok::text::jsonb')
    op.alter_column('meetbouten_meetbouten', 'ligt_in_buurt', type_=postgresql.JSONB, postgresql_using='ligt_in_buurt::text::jsonb')
    op.alter_column('meetbouten_meetbouten', 'ligt_in_stadsdeel', type_=postgresql.JSONB, postgresql_using='ligt_in_stadsdeel::text::jsonb')

    op.alter_column('meetbouten_metingen', 'hoort_bij_meetbout', type_=postgresql.JSONB, postgresql_using='hoort_bij_meetbout::text::jsonb')
    op.alter_column('meetbouten_metingen', 'wijze_van_inwinnen', type_=postgresql.JSONB, postgresql_using='wijze_van_inwinnen::text::jsonb')
    op.alter_column('meetbouten_metingen', 'refereert_aan', type_=postgresql.JSONB, postgresql_using='refereert_aan::text::jsonb')

    op.alter_column('meetbouten_referentiepunten', 'nabij_nummeraanduiding', type_=postgresql.JSONB, postgresql_using='nabij_nummeraanduiding::text::jsonb')
    op.alter_column('meetbouten_referentiepunten', 'status', type_=postgresql.JSONB, postgresql_using='status::text::jsonb')
    op.alter_column('meetbouten_referentiepunten', 'merk', type_=postgresql.JSONB, postgresql_using='merk::text::jsonb')
    op.alter_column('meetbouten_referentiepunten', 'ligt_in_bouwblok', type_=postgresql.JSONB, postgresql_using='ligt_in_bouwblok::text::jsonb')
    op.alter_column('meetbouten_referentiepunten', 'ligt_in_buurt', type_=postgresql.JSONB, postgresql_using='ligt_in_buurt::text::jsonb')
    op.alter_column('meetbouten_referentiepunten', 'ligt_in_stadsdeel', type_=postgresql.JSONB, postgresql_using='ligt_in_stadsdeel::text::jsonb')
    op.alter_column('meetbouten_referentiepunten', 'is_nap_peilmerk', type_=postgresql.JSONB, postgresql_using='is_nap_peilmerk::text::jsonb')

    op.alter_column('meetbouten_rollagen', 'is_gemeten_van_bouwblok', type_=postgresql.JSONB, postgresql_using='is_gemeten_van_bouwblok::text::jsonb')

    op.alter_column('nap_peilmerken', 'merk', type_=postgresql.JSONB, postgresql_using='merk::text::jsonb')
    op.alter_column('nap_peilmerken', 'status', type_=postgresql.JSONB, postgresql_using='status::text::jsonb')
    op.alter_column('nap_peilmerken', 'ligt_in_bouwblok', type_=postgresql.JSONB, postgresql_using='ligt_in_bouwblok::text::jsonb')


def downgrade():
    # Drop views to allow columns to be changed, GOB-Upload will recreate them
    op.execute('drop view meetbouten_meetbouten_enhanced')
    op.execute('drop view meetbouten_metingen_enhanced')
    op.execute('drop view meetbouten_referentiepunten_enhanced')
    op.execute('drop view meetbouten_rollagen_enhanced')
    op.execute('drop view nap_peilmerken_enhanced')

    # Change all JSONB columns to JSON
    op.alter_column('meetbouten_meetbouten', 'nabij_nummeraanduiding', type_=postgresql.JSONB, postgresql_using='nabij_nummeraanduiding::text::json')
    op.alter_column('meetbouten_meetbouten', 'status', type_=postgresql.JSONB, postgresql_using='status::text::json')
    op.alter_column('meetbouten_meetbouten', 'merk', type_=postgresql.JSONB, postgresql_using='merk::text::json')
    op.alter_column('meetbouten_meetbouten', 'ligt_in_bouwblok', type_=postgresql.JSONB, postgresql_using='ligt_in_bouwblok::text::json')
    op.alter_column('meetbouten_meetbouten', 'ligt_in_buurt', type_=postgresql.JSONB, postgresql_using='ligt_in_buurt::text::json')
    op.alter_column('meetbouten_meetbouten', 'ligt_in_stadsdeel', type_=postgresql.JSONB, postgresql_using='ligt_in_stadsdeel::text::json')

    op.alter_column('meetbouten_metingen', 'hoort_bij_meetbout', type_=postgresql.JSONB, postgresql_using='hoort_bij_meetbout::text::json')
    op.alter_column('meetbouten_metingen', 'wijze_van_inwinnen', type_=postgresql.JSONB, postgresql_using='wijze_van_inwinnen::text::json')
    op.alter_column('meetbouten_metingen', 'refereert_aan', type_=postgresql.JSONB, postgresql_using='refereert_aan::text::json')

    op.alter_column('meetbouten_referentiepunten', 'nabij_nummeraanduiding', type_=postgresql.JSONB, postgresql_using='nabij_nummeraanduiding::text::json')
    op.alter_column('meetbouten_referentiepunten', 'status', type_=postgresql.JSONB, postgresql_using='status::text::json')
    op.alter_column('meetbouten_referentiepunten', 'merk', type_=postgresql.JSONB, postgresql_using='merk::text::json')
    op.alter_column('meetbouten_referentiepunten', 'ligt_in_bouwblok', type_=postgresql.JSONB, postgresql_using='ligt_in_bouwblok::text::json')
    op.alter_column('meetbouten_referentiepunten', 'ligt_in_buurt', type_=postgresql.JSONB, postgresql_using='ligt_in_buurt::text::json')
    op.alter_column('meetbouten_referentiepunten', 'ligt_in_stadsdeel', type_=postgresql.JSONB, postgresql_using='ligt_in_stadsdeel::text::json')
    op.alter_column('meetbouten_referentiepunten', 'is_nap_peilmerk', type_=postgresql.JSONB, postgresql_using='is_nap_peilmerk::text::json')

    op.alter_column('meetbouten_rollagen', 'is_gemeten_van_bouwblok', type_=postgresql.JSONB, postgresql_using='is_gemeten_van_bouwblok::text::json')

    op.alter_column('nap_peilmerken', 'merk', type_=postgresql.JSONB, postgresql_using='merk::text::json')
    op.alter_column('nap_peilmerken', 'status', type_=postgresql.JSONB, postgresql_using='status::text::json')
    op.alter_column('nap_peilmerken', 'ligt_in_bouwblok', type_=postgresql.JSONB, postgresql_using='ligt_in_bouwblok::text::json')
