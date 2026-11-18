"""initial notifications schema

Revision ID: 0001_initial
Revises:
"""
from alembic import op
from src.core.db.models import metadata
revision = '0001_initial'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    bind = op.get_bind()
    metadata.create_all(bind=bind)

def downgrade():
    raise NotImplementedError('Dropping notifications tables would delete user and delivery data')
