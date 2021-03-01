"""correct version numbers

Revision ID: ceab55579508
Revises: 7e355e481102
Create Date: 2021-03-01 08:42:33.376041

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from gobupload.correct_version_numbers import correct_version_numbers


# revision identifiers, used by Alembic.
revision = 'ceab55579508'
down_revision = '7e355e481102'
branch_labels = None
depends_on = None

def upgrade():
    print("Correcting version numbers. This may take a while. Please be patient :)")
    correct_version_numbers()


def downgrade():
    pass
