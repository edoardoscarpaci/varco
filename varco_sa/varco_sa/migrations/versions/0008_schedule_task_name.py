"""schedules.task_name — Plan 039 (S20) / §D-S20-driver "(1)"

Revision ID: 0008_schedule_task_name
Revises: 0007_schedules_table
Create Date: 2026-09-09

Adds a single nullable column, ``task_name``, to the ``schedules`` table
(``varco_sa/schedule.py``). No backfill, no default-value change: every
existing row gets ``NULL``, which is exactly what ``Schedule.task_name =
None`` already means — a ``Schedule`` with no executable body, materialized
as a ``Job`` with ``task_payload=None``, byte-identical to pre-3.2
behaviour (``varco_core/tests/test_schedule_materializer.py``'s
``test_task_name_none_produces_no_task_payload_pinned`` pins this).

**Why the idempotent column-exists guard is mandatory, not stylistic**
(same reasoning as ``0004_job_zoned_schedule.py``, ``0002_dlq_audit_tenant_id``,
``0003_audit_hash_chain``): ``0001_varco_framework_baseline`` is dynamic —
it iterates ``varco_sa.metadata.framework_metadata().tables`` and creates
whatever the *installed wheel* declares. Since ``schedule.py``'s
``_schedules_table`` Table object now declares ``task_name``, a FRESH
database created by ``0001`` (or by ``0007_schedules_table``'s own
``checkfirst=True`` create, which reads the same live metadata) already has
the column, and this revision must be a no-op there. A database stamped at
``0007`` before upgrading does NOT have it, and this revision adds it. Both
paths converge on the same schema.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0008_schedule_task_name"
down_revision = "0007_schedules_table"
branch_labels = None
depends_on = None

_TABLE = "schedules"
_COLUMN = "task_name"


def _has_column(bind: sa.engine.Connection, table: str, column: str) -> bool:
    inspector = sa.inspect(bind)
    if table not in inspector.get_table_names():
        return False
    return column in {c["name"] for c in inspector.get_columns(table)}


def upgrade() -> None:
    bind = op.get_bind()
    if not _has_column(bind, _TABLE, _COLUMN):
        op.add_column(_TABLE, sa.Column(_COLUMN, sa.String(length=255), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    if _has_column(bind, _TABLE, _COLUMN):
        op.drop_column(_TABLE, _COLUMN)
