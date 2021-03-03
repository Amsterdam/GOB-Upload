"""remove underscores from hr

Revision ID: a2547db51a6a
Revises: ceab55579508
Create Date: 2021-03-03 13:23:15.100799

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'a2547db51a6a'
down_revision = 'ceab55579508'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('hr_maatschappelijkeactiviteiten',
    sa.Column('kvknummer', sa.String(), autoincrement=False, nullable=True),
    sa.Column('datum_aanvang_maatschappelijke_activiteit', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('datum_einde_maatschappelijke_activiteit', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('registratie_tijdstip_maatschappelijke_activiteit', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('naam', sa.String(), autoincrement=False, nullable=True),
    sa.Column('heeft_bezoekadres', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('heeft_postadres', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('communicatienummer', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('domeinnaam', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('email_adres', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('non_mailing', sa.Boolean(), autoincrement=False, nullable=True),
    sa.Column('incidenteel_uitlenen_arbeidskrachten', sa.Boolean(), autoincrement=False, nullable=True),
    sa.Column('heeft_hoofdvestiging', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('heeft_sbi_activiteiten_voor_maatschappelijke_activiteit', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('registratie_tijdstip_onderneming', sa.DateTime(), autoincrement=False, nullable=True),
    sa.Column('datum_aanvang_onderneming', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('datum_einde_onderneming', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('is_overdracht_voortzetting_onderneming', sa.Boolean(), autoincrement=False, nullable=True),
    sa.Column('datum_overdracht_voortzetting_onderneming', sa.Date(), autoincrement=False, nullable=True),
    sa.Column('totaal_werkzame_personen', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('voltijd_werkzame_personen', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('deeltijd_werkzame_personen', sa.Integer(), autoincrement=False, nullable=True),
    sa.Column('heeft_sbi_activiteiten_voor_onderneming', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('wordt_uitgeoefend_in_commerciele_vestiging', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('wordt_uitgeoefend_in_niet_commerciele_vestiging', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('handelt_onder_handelsnamen', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
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
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_id', name='hr_maatschappelijkeactiviteiten__id_key')
    )
    op.create_table('qa_hr_maatschappelijkeactiviteiten',
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
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_id', name='qa_hr_maatschappelijkeactiviteiten__id_key')
    )
    op.create_table('rel_brk_sjt_hr_mac_heeft_kvknummer_voor',
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
    sa.ForeignKeyConstraint(['dst_id'], ['hr_maatschappelijkeactiviteiten._id'], name='rel_brk_sjt_hr_mac_heeft_kvknummer_voor_dfk'),
    sa.ForeignKeyConstraint(['src_id'], ['brk_kadastralesubjecten._id'], name='rel_brk_sjt_hr_mac_heeft_kvknummer_voor_sfk'),
    sa.PrimaryKeyConstraint('_gobid'),
    sa.UniqueConstraint('_source_id', name='rel_brk_sjt_hr_mac_heeft_kvknummer_voor_uniq')
    )
    op.drop_table('qa_hr_maatschappelijke_activiteiten')
    op.drop_table('hr_maatschappelijke_activiteiten')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('hr_maatschappelijke_activiteiten',
    sa.Column('kvknummer', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('datum_aanvang_maatschappelijke_activiteit', sa.DATE(), autoincrement=False, nullable=True),
    sa.Column('datum_einde_maatschappelijke_activiteit', sa.DATE(), autoincrement=False, nullable=True),
    sa.Column('registratie_tijdstip_maatschappelijke_activiteit', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('naam', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('heeft_bezoekadres', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('heeft_postadres', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('communicatienummer', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('domeinnaam', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('email_adres', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('non_mailing', sa.BOOLEAN(), autoincrement=False, nullable=True),
    sa.Column('incidenteel_uitlenen_arbeidskrachten', sa.BOOLEAN(), autoincrement=False, nullable=True),
    sa.Column('heeft_hoofdvestiging', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('heeft_sbi_activiteiten_voor_maatschappelijke_activiteit', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('registratie_tijdstip_onderneming', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('datum_aanvang_onderneming', sa.DATE(), autoincrement=False, nullable=True),
    sa.Column('datum_einde_onderneming', sa.DATE(), autoincrement=False, nullable=True),
    sa.Column('is_overdracht_voortzetting_onderneming', sa.BOOLEAN(), autoincrement=False, nullable=True),
    sa.Column('datum_overdracht_voortzetting_onderneming', sa.DATE(), autoincrement=False, nullable=True),
    sa.Column('totaal_werkzame_personen', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('voltijd_werkzame_personen', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('deeltijd_werkzame_personen', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('heeft_sbi_activiteiten_voor_onderneming', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('wordt_uitgeoefend_in_commerciele_vestiging', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('wordt_uitgeoefend_in_niet_commerciele_vestiging', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('handelt_onder_handelsnamen', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('_source', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_application', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_source_id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_last_event', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('_hash', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_version', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_date_created', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_date_confirmed', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_date_modified', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_date_deleted', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_expiration_date', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_gobid', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('_id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('_gobid', name='hr_maatschappelijke_activiteiten_pkey'),
    sa.UniqueConstraint('_id', name='hr_maatschappelijke_activiteiten__id_key')
    )
    op.create_table('qa_hr_maatschappelijke_activiteiten',
    sa.Column('meldingnummer', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('code', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('proces', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('attribuut', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('identificatie', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('volgnummer', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('begin_geldigheid', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('eind_geldigheid', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('betwijfelde_waarde', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('onderbouwing', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('voorgestelde_waarde', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_source', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_application', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_source_id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_last_event', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('_hash', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_version', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('_date_created', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_date_confirmed', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_date_modified', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_date_deleted', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_expiration_date', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('_gobid', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('_id', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('_gobid', name='qa_hr_maatschappelijke_activiteiten_pkey'),
    sa.UniqueConstraint('_id', name='qa_hr_maatschappelijke_activiteiten__id_key')
    )
    op.drop_table('rel_brk_sjt_hr_mac_heeft_kvknummer_voor')
    op.drop_table('qa_hr_maatschappelijkeactiviteiten')
    op.drop_table('hr_maatschappelijkeactiviteiten')
    # ### end Alembic commands ###
