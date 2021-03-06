"""baseline

Revision ID: fc690dd4ed41
Revises: 
Create Date: 2018-11-20 13:48:08.867181

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = 'fc690dd4ed41'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('events',
    sa.Column('eventid', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('timestamp', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('catalogue', sa.String(), autoincrement=False, nullable=True),
    sa.Column('entity', sa.String(), autoincrement=False, nullable=True),
    sa.Column('version', sa.String(), autoincrement=False, nullable=True),
    sa.Column('action', sa.String(), autoincrement=False, nullable=True),
    sa.Column('source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('source_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('contents', sa.JSON(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('eventid')
    )
    op.create_table('meetbouten_meetbouten',
    sa.Column('_gobid', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_last_event', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('_version', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_date_created', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_confirmed', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_modified', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_deleted', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('identificatie', sa.String(), autoincrement=False, nullable=True),
    sa.Column('nabij_nummeraanduiding', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('locatie', sa.String(), autoincrement=False, nullable=True),
    sa.Column('status', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('vervaldatum', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('merk', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('x_coordinaat_muurvlak', sa.DECIMAL(), autoincrement=False, nullable=True),
    sa.Column('y_coordinaat_muurvlak', sa.DECIMAL(), autoincrement=False, nullable=True),
    sa.Column('windrichting', sa.String(), autoincrement=False, nullable=True),
    sa.Column('ligt_in_bouwblok', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('ligt_in_buurt', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('ligt_in_stadsdeel', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('geometrie', geoalchemy2.types.Geometry(geometry_type='POINT', srid=28992), nullable=True),
    sa.Column('publiceerbaar', sa.Boolean(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('_gobid')
    )
    op.create_table('meetbouten_metingen',
    sa.Column('_gobid', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_last_event', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('_version', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_date_created', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_confirmed', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_modified', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_deleted', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('identificatie', sa.String(), autoincrement=False, nullable=True),
    sa.Column('hoort_bij_meetbout', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('datum', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('type_meting', sa.CHAR(), autoincrement=False, nullable=True),
    sa.Column('wijze_van_inwinnen', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('hoogte_tov_nap', sa.DECIMAL(), autoincrement=False, nullable=True),
    sa.Column('zakking', sa.DECIMAL(), autoincrement=False, nullable=True),
    sa.Column('refereert_aan', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('zakkingssnelheid', sa.DECIMAL(), autoincrement=False, nullable=True),
    sa.Column('zakking_cumulatief', sa.DECIMAL(), autoincrement=False, nullable=True),
    sa.Column('is_gemeten_door', sa.String(), autoincrement=False, nullable=True),
    sa.Column('hoeveelste_meting', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('aantal_dagen', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('publiceerbaar', sa.Boolean(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('_gobid')
    )
    op.create_table('meetbouten_referentiepunten',
    sa.Column('_gobid', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_last_event', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('_version', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_date_created', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_confirmed', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_modified', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_deleted', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('identificatie', sa.String(), autoincrement=False, nullable=True),
    sa.Column('nabij_nummeraanduiding', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('locatie', sa.String(), autoincrement=False, nullable=True),
    sa.Column('hoogte_tov_nap', sa.DECIMAL(), autoincrement=False, nullable=True),
    sa.Column('datum', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('status', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('vervaldatum', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('merk', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('x_coordinaat_muurvlak', sa.DECIMAL(), autoincrement=False, nullable=True),
    sa.Column('y_coordinaat_muurvlak', sa.DECIMAL(), autoincrement=False, nullable=True),
    sa.Column('windrichting', sa.String(), autoincrement=False, nullable=True),
    sa.Column('ligt_in_bouwblok', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('ligt_in_buurt', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('ligt_in_stadsdeel', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('geometrie', geoalchemy2.types.Geometry(geometry_type='POINT', srid=28992), nullable=True),
    sa.Column('is_nap_peilmerk', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('publiceerbaar', sa.Boolean(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('_gobid')
    )
    op.create_table('meetbouten_rollagen',
    sa.Column('_gobid', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_last_event', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('_version', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_date_created', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_confirmed', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_modified', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_deleted', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('identificatie', sa.String(), autoincrement=False, nullable=True),
    sa.Column('is_gemeten_van_bouwblok', sa.JSON(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('_gobid')
    )
    op.create_table('nap_peilmerken',
    sa.Column('_gobid', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_last_event', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('_version', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_date_created', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_confirmed', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_modified', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_deleted', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('identificatie', sa.String(), autoincrement=False, nullable=True),
    sa.Column('hoogte_tov_nap', sa.DECIMAL(), autoincrement=False, nullable=True),
    sa.Column('jaar', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('merk', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('omschrijving', sa.String(), autoincrement=False, nullable=True),
    sa.Column('windrichting', sa.String(), autoincrement=False, nullable=True),
    sa.Column('x_coordinaat_muurvlak', sa.DECIMAL(), autoincrement=False, nullable=True),
    sa.Column('y_coordinaat_muurvlak', sa.DECIMAL(), autoincrement=False, nullable=True),
    sa.Column('rws_nummer', sa.String(), autoincrement=False, nullable=True),
    sa.Column('geometrie', geoalchemy2.types.Geometry(geometry_type='POINT', srid=28992), nullable=True),
    sa.Column('status', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('vervaldatum', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('ligt_in_bouwblok', sa.JSON(), autoincrement=False, nullable=True),
    sa.Column('publiceerbaar', sa.Boolean(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('_gobid')
    )
    op.create_table('test_catalogue_test_entity',
    sa.Column('_gobid', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_source_id', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_last_event', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('_version', sa.String(), autoincrement=False, nullable=True),
    sa.Column('_date_created', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_confirmed', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_modified', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('_date_deleted', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('string', sa.String(), autoincrement=False, nullable=True),
    sa.Column('character', sa.CHAR(), autoincrement=False, nullable=True),
    sa.Column('decimal', sa.DECIMAL(), autoincrement=False, nullable=True),
    sa.Column('integer', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('point', geoalchemy2.types.Geometry(geometry_type='POINT', srid=28992), nullable=True),
    sa.Column('date', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('boolean', sa.Boolean(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('_gobid')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('test_catalogue_test_entity')
    op.drop_table('nap_peilmerken')
    op.drop_table('meetbouten_rollagen')
    op.drop_table('meetbouten_referentiepunten')
    op.drop_table('meetbouten_metingen')
    op.drop_table('meetbouten_meetbouten')
    op.drop_table('events')
    # ### end Alembic commands ###
