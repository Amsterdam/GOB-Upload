"""BAG changes

Revision ID: f0a42ec49f4c
Revises: 0ab61018638e
Create Date: 2019-03-11 11:48:36.945569

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'f0a42ec49f4c'
down_revision = '0ab61018638e'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('bag_brondocumenten',
    sa.Column('_gobid', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('_id', sa.String(), autoincrement=False, nullable=True),
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
    sa.Column('documentnummer', sa.String(), autoincrement=False, nullable=True),
    sa.Column('dossier', sa.String(), autoincrement=False, nullable=True),
    sa.Column('bronleverancier', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('type_dossier', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('type_brondocument', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('registratiedatum', sa.DateTime(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('_gobid')
    )
    op.alter_column('bag_openbareruimtes', 'beschrijving', nullable=True, new_column_name='beschrijving_naam')
    op.alter_column('bag_verblijfsobjecten', 'gebruiksdoel_zorgfunctie', nullable=True, new_column_name='gebruiksdoel_gezondheidszorgfunctie')
    op.add_column('bag_verblijfsobjecten', sa.Column('eigendomsverhouding', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('bag_openbareruimtes', 'beschrijving_naam', nullable=True, new_column_name='beschrijving')
    op.alter_column('bag_verblijfsobjecten', 'gebruiksdoel_gezondheidszorgfunctie', nullable=True, new_column_name='gebruiksdoel_zorgfunctie')
    op.drop_column('bag_verblijfsobjecten', 'eigendomsverhouding')
    op.drop_table('bag_brondocumenten')
    # ### end Alembic commands ###