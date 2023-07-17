"""add brk2 erfpachtcanons

Revision ID: dae375bb75c9
Revises: 19f159c5e167
Create Date: 2023-07-17 17:07:07.612331

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'dae375bb75c9'
down_revision = '19f159c5e167'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        'brk2_erfpachtcanons',
        sa.Column('volgnummer', sa.Integer(), autoincrement=False, nullable=True),
        sa.Column('identificatie', sa.String(), autoincrement=False, nullable=True),
        sa.Column('soort', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
        sa.Column('jaarlijks_bedrag', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False,
                  nullable=True),
        sa.Column('is_gebaseerd_op_brk_stukdeel', postgresql.JSONB(astext_type=sa.Text()),
                  autoincrement=False, nullable=True),
        sa.Column('einddatum', sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column('datum_actueel_tot', sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column('toestandsdatum', sa.Date(), autoincrement=False, nullable=True),
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
        sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column('_gobid', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('_id', sa.String(), autoincrement=False, nullable=True),
        sa.Column('_tid', sa.String(), autoincrement=False, nullable=True),
        sa.PrimaryKeyConstraint('_gobid'),
        sa.UniqueConstraint('_id', name='brk2_erfpachtcanons__id_key'),
        sa.UniqueConstraint('_tid', name='brk2_erfpachtcanons__tid_key')
    )
    op.create_table(
        'qa_brk2_erfpachtcanons',
        sa.Column('meldingnummer', sa.String(), autoincrement=False, nullable=True),
        sa.Column('code', sa.String(), autoincrement=False, nullable=True),
        sa.Column('proces', sa.String(), autoincrement=False, nullable=True),
        sa.Column('attribuut', sa.String(), autoincrement=False, nullable=True),
        sa.Column('identificatie', sa.String(), autoincrement=False, nullable=True),
        sa.Column('volgnummer', sa.Integer(), autoincrement=False, nullable=True),
        sa.Column('begin_geldigheid', sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column('eind_geldigheid', sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column('betwijfelde_waarde', sa.String(), autoincrement=False, nullable=True),
        sa.Column('onderbouwing', sa.String(), autoincrement=False, nullable=True),
        sa.Column('voorgestelde_waarde', sa.String(), autoincrement=False, nullable=True),
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
        sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column('_gobid', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('_id', sa.String(), autoincrement=False, nullable=True),
        sa.Column('_tid', sa.String(), autoincrement=False, nullable=True),
        sa.PrimaryKeyConstraint('_gobid'),
        sa.UniqueConstraint('_id', name='qa_brk2_erfpachtcanons__id_key'),
        sa.UniqueConstraint('_tid', name='qa_brk2_erfpachtcanons__tid_key')
    )
    op.create_table(
        'rel_brk2_ecs_brk2_sdl_is_gebaseerd_op_brk_stukdeel',
        sa.Column('id', sa.String(), autoincrement=False, nullable=True),
        sa.Column('src_source', sa.String(), autoincrement=False, nullable=True),
        sa.Column('src_id', sa.String(), autoincrement=False, nullable=True),
        sa.Column('src_volgnummer', sa.Integer(), autoincrement=False, nullable=True),
        sa.Column('bronwaarde', sa.String(), autoincrement=False, nullable=True),
        sa.Column('derivation', sa.String(), autoincrement=False, nullable=True),
        sa.Column('dst_source', sa.String(), autoincrement=False, nullable=True),
        sa.Column('dst_id', sa.String(), autoincrement=False, nullable=True),
        sa.Column('dst_volgnummer', sa.Integer(), autoincrement=False, nullable=True),
        sa.Column('begin_geldigheid', sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column('eind_geldigheid', sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column('_last_src_event', sa.Integer(), autoincrement=False, nullable=True),
        sa.Column('_last_dst_event', sa.Integer(), autoincrement=False, nullable=True),
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
        sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column('_gobid', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('_id', sa.String(), autoincrement=False, nullable=True),
        sa.Column('_tid', sa.String(), autoincrement=False, nullable=True),
        sa.ForeignKeyConstraint(['dst_id'], ['brk2_stukdelen._id'],
                                name='rel_brk2_ecs_brk2_sdl_is_gebaseerd_op_brk_stukdeel_dfk'),
        sa.ForeignKeyConstraint(['src_id'], ['brk2_erfpachtcanons._id'],
                                name='rel_brk2_ecs_brk2_sdl_is_gebaseerd_op_brk_stukdeel_sfk'),
        sa.PrimaryKeyConstraint('_gobid'),
        sa.UniqueConstraint('_source_id', name='rel_brk2_ecs_brk2_sdl_is_gebaseerd_op_brk_stukdeel_uniq'),
        sa.UniqueConstraint('_tid', name='rel_brk2_ecs_brk2_sdl_is_gebaseerd_op_brk_stukdeel__tid_key')
    )
    op.create_table(
        'rel_brk2_sdl_brk2_ecs_is_bron_voor_brk_erfpachtcanon',
        sa.Column('id', sa.String(), autoincrement=False, nullable=True),
        sa.Column('src_source', sa.String(), autoincrement=False, nullable=True),
        sa.Column('src_id', sa.String(), autoincrement=False, nullable=True),
        sa.Column('src_volgnummer', sa.Integer(), autoincrement=False, nullable=True),
        sa.Column('bronwaarde', sa.String(), autoincrement=False, nullable=True),
        sa.Column('derivation', sa.String(), autoincrement=False, nullable=True),
        sa.Column('dst_source', sa.String(), autoincrement=False, nullable=True),
        sa.Column('dst_id', sa.String(), autoincrement=False, nullable=True),
        sa.Column('dst_volgnummer', sa.Integer(), autoincrement=False, nullable=True),
        sa.Column('begin_geldigheid', sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column('eind_geldigheid', sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column('_last_src_event', sa.Integer(), autoincrement=False, nullable=True),
        sa.Column('_last_dst_event', sa.Integer(), autoincrement=False, nullable=True),
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
        sa.Column('_expiration_date', sa.DateTime(), autoincrement=False, nullable=True),
        sa.Column('_gobid', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('_id', sa.String(), autoincrement=False, nullable=True),
        sa.Column('_tid', sa.String(), autoincrement=False, nullable=True),
        sa.ForeignKeyConstraint(['dst_id'], ['brk2_erfpachtcanons._id'],
                                name='rel_brk2_sdl_brk2_ecs_is_bron_voor_brk_erfpachtcanon_dfk'),
        sa.ForeignKeyConstraint(['src_id'], ['brk2_stukdelen._id'],
                                name='rel_brk2_sdl_brk2_ecs_is_bron_voor_brk_erfpachtcanon_sfk'),
        sa.PrimaryKeyConstraint('_gobid'),
        sa.UniqueConstraint('_source_id', name='rel_brk2_sdl_brk2_ecs_is_bron_voor_brk_erfpachtcanon_uniq'),
        sa.UniqueConstraint('_tid', name='rel_brk2_sdl_brk2_ecs_is_bron_voor_brk_erfpachtcanon__tid_key')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('rel_brk2_sdl_brk2_ecs_is_bron_voor_brk_erfpachtcanon')
    op.drop_table('rel_brk2_ecs_brk2_sdl_is_gebaseerd_op_brk_stukdeel')
    op.drop_table('qa_brk2_erfpachtcanons')
    op.drop_table('brk2_erfpachtcanons')
    # ### end Alembic commands ###
