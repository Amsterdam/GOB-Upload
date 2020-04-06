"""Clear wrongly imported relations

Revision ID: b83e1c7228db
Revises: bc1071e546f3
Create Date: 2020-03-31 13:03:23.401391

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2

from gobcore.model import GOBModel
from gobcore.model.relations import get_relations

# revision identifiers, used by Alembic.
revision = 'b83e1c7228db'
down_revision = 'bc1071e546f3'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###

    model = GOBModel()

    connection = op.get_bind()

    for relation_name, relation in get_relations(model)['collections'].items():
        table_name = f"rel_{relation_name}"

        res = connection.execute(f"SELECT * FROM {table_name} WHERE bronwaarde IS NULL LIMIT 1")

        if res.fetchall():
            print(f"{table_name} contains NULL values for bronwaarde. Clearing table and events.")

            op.execute(f"DELETE FROM events WHERE entity='{relation_name}'")
            op.execute(f"TRUNCATE {table_name}")

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###