"""Change incompletedate type

Revision ID: 9a5f8a3bd635
Revises: ea57833352e1
Create Date: 2021-03-23 09:54:33.477268

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '9a5f8a3bd635'
down_revision = 'ea57833352e1'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("UPDATE test_catalogue_test_entity SET incomplete_date = NULL")
    op.alter_column('test_catalogue_test_entity', 'incomplete_date', type_=postgresql.JSONB, postgresql_using='incomplete_date::jsonb')

def downgrade():
    op.alter_column('test_catalogue_test_entity', 'incomplete_date', type_=sa.String())
