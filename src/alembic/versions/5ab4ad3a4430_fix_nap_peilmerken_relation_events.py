"""fix nap peilmerken relation events

Revision ID: 5ab4ad3a4430
Revises: bf1c254bb83a
Create Date: 2022-08-29 12:50:46.357365

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '5ab4ad3a4430'
down_revision = 'bf1c254bb83a'
branch_labels = None
depends_on = None


def upgrade():
    # This code is an adjusted version of _rename_relation in alembic_utils.py.
    # DO NOT use this migration when renaming relations, because additional steps are necessary.
    # This migration is just to fix the previous (incomplete) migration

    # Search for the usage of upgrade_relations and downgrade_relations in other revisions on how to do this properly

    # :)

    old_relation_table = "rel_nap_pmk_gbd_bbk_ligt_in_bouwblok"
    new_relation_table = "rel_nap_pmk_gbd_bbk_ligt_in_gebieden_bouwblok"

    old_relation_name = old_relation_table.replace("rel_", "", 1)
    new_relation_name = new_relation_table.replace("rel_", "", 1)
    rename_events_query = f"UPDATE events SET entity = '{new_relation_name}' " \
                          f"WHERE catalogue='rel' AND entity = '{old_relation_name}'"

    # Create partitions for events if they don't exist yet
    op.execute(
        "CREATE TABLE IF NOT EXISTS events.rel PARTITION OF events FOR VALUES IN ('rel') PARTITION BY LIST (entity)")
    op.execute(
        f"CREATE TABLE IF NOT EXISTS events.rel_{new_relation_name} PARTITION OF events.rel "
        f"FOR VALUES IN ('{new_relation_name}') PARTITION BY LIST(source)")
    op.execute(
        f"CREATE TABLE IF NOT EXISTS events.rel_{new_relation_name}_gob PARTITION OF events.rel_{new_relation_name} "
        f"FOR VALUES IN ('GOB')")
    op.execute(rename_events_query)


def downgrade():
    pass
