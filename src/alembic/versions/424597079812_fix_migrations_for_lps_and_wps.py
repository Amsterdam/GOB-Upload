"""Fix migrations for lps and wps

Revision ID: 424597079812
Revises: a2547db51a6a
Create Date: 2021-03-09 16:01:29.376328

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2
from gobupload.correct_version_numbers import correct_version_numbers

# revision identifiers, used by Alembic.
revision = '424597079812'
down_revision = 'a2547db51a6a'
branch_labels = None
depends_on = None


def upgrade():
    print("Correcting version numbers. This may take a while. Please be patient :)")
    correct_version_numbers()


def downgrade():
    pass
