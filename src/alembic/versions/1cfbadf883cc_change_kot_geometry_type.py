"""Change kot geometry type

Revision ID: 1cfbadf883cc
Revises: 088e1b793558
Create Date: 2019-11-20 10:49:08.338560

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '1cfbadf883cc'
down_revision = '088e1b793558'
branch_labels = None
depends_on = None


def upgrade():
    # Drop views to allow columns to be changed, GOB-Upload will recreate them
    op.execute('DROP VIEW IF EXISTS wkpb_beperkingen_enhanced')

    op.alter_column('brk_kadastraleobjecten', 'geometrie', type_=geoalchemy2.types.Geometry(srid=28992))
    op.alter_column('bag_verblijfsobjecten', 'woningvoorraad', new_column_name='indicatie_woningvoorraad')


def downgrade():
    # Drop views to allow columns to be changed, GOB-Upload will recreate them
    op.execute('DROP VIEW IF EXISTS wkpb_beperkingen_enhanced')

    op.alter_column('brk_kadastraleobjecten', 'geometrie', type_=geoalchemy2.types.Geometry(geometry_type='POLYGON', srid=28992))
    op.alter_column('bag_verblijfsobjecten', 'indicatie_woningvoorraad', new_column_name='woningvoorraad')
