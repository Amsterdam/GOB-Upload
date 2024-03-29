"""migrate nap peilmerken to ams schema

Revision ID: dc61bf7df671
Revises: 8469e1243db6
Create Date: 2022-07-12 09:02:32.690710

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql
from gobupload.alembic_utils import get_query_split_json_column, get_query_merge_columns_to_jsonb_column, \
    RenamedRelation, upgrade_relations, downgrade_relations

# revision identifiers, used by Alembic.
revision = 'dc61bf7df671'
down_revision = '8469e1243db6'
branch_labels = None
depends_on = None

renamed_relations = [
    RenamedRelation(
        table_name='nap_peilmerken',
        old_column='ligt_in_bouwblok',
        new_column='ligt_in_gebieden_bouwblok',
        old_relation_table='rel_nap_pmk_gbd_bbk_ligt_in_bouwblok',
        new_relation_table='rel_nap_pmk_gbd_bbk_ligt_in_gebieden_bouwblok'
    )
]


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###

    upgrade_relations(op, renamed_relations)

    op.add_column('nap_peilmerken', sa.Column('merk_code', sa.String(), autoincrement=False, nullable=True))
    op.add_column('nap_peilmerken', sa.Column('merk_omschrijving', sa.String(), autoincrement=False, nullable=True))
    op.add_column('nap_peilmerken', sa.Column('status_code', sa.Integer(), autoincrement=False, nullable=True))
    op.add_column('nap_peilmerken', sa.Column('status_omschrijving', sa.String(), autoincrement=False, nullable=True))
    op.add_column('nap_peilmerken', sa.Column('datum_actueel_tot', sa.DateTime(), autoincrement=False, nullable=True))

    op.execute(get_query_split_json_column('nap_peilmerken', 'status', {'code': 'status_code', 'omschrijving': 'status_omschrijving'}, {'code': 'int', 'omschrijving': 'varchar'}))
    op.execute(get_query_split_json_column('nap_peilmerken', 'merk', {'code': 'merk_code', 'omschrijving': 'merk_omschrijving'}, {'code': 'varchar', 'omschrijving': 'varchar'}))

    op.execute("ALTER TABLE nap_peilmerken DROP COLUMN status CASCADE")
    op.execute("ALTER TABLE nap_peilmerken DROP COLUMN merk CASCADE")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('nap_peilmerken', sa.Column('merk', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    op.add_column('nap_peilmerken', sa.Column('status', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))

    op.execute(get_query_merge_columns_to_jsonb_column('nap_peilmerken', 'merk', {'code': 'merk_code', 'omschrijving': 'merk_omschrijving'}))
    op.execute(get_query_merge_columns_to_jsonb_column('nap_peilmerken', 'status', {'code': 'status_code', 'omschrijving': 'status_omschrijving'}))

    op.drop_column('nap_peilmerken', 'datum_actueel_tot')
    op.drop_column('nap_peilmerken', 'status_omschrijving')
    op.drop_column('nap_peilmerken', 'status_code')
    op.drop_column('nap_peilmerken', 'merk_omschrijving')
    op.drop_column('nap_peilmerken', 'merk_code')

    downgrade_relations(op, renamed_relations)
    # ### end Alembic commands ###
