"""fernet

Revision ID: 8bb410260f51
Revises: 1cfbadf883cc
Create Date: 2019-11-22 15:48:51.320033

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '8bb410260f51'
down_revision = '1cfbadf883cc'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("TRUNCATE TABLE test_catalogue_secure;commit;")
    op.alter_column('test_catalogue_secure', 'secure_string', type_=sa.String())
    op.alter_column('test_catalogue_secure', 'secure_number', type_=sa.String())
    op.alter_column('test_catalogue_secure', 'secure_datetime', type_=sa.String())
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('test_catalogue_secure', 'secure_string', type_=postgresql.JSONB(astext_type=sa.Text()))
    op.alter_column('test_catalogue_secure', 'secure_number', type_=postgresql.JSONB(astext_type=sa.Text()))
    op.alter_column('test_catalogue_secure', 'secure_datetime', type_=postgresql.JSONB(astext_type=sa.Text()))
    # ### end Alembic commands ###