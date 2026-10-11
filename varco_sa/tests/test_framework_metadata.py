"""
Failing tests for varco_sa.metadata (Plan 006, Phase 0, steps 6 and 7).

``framework_metadata()`` is the single aggregated ``MetaData`` covering every
framework-owned table (outbox, inbox, jobs, sagas, conversation, dedup,
audit, dlq, encryption keys). This module has two jobs:

1. A completeness guard — every module-level ``MetaData``/``DeclarativeBase``
   table in ``varco_sa`` must be a subset of ``framework_metadata().tables``.
2. An exact-name guard — the literal table names must not silently drift.
"""

from __future__ import annotations

import importlib
import pkgutil

import pytest
from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

import varco_sa

# Table names read literally from source (Plan 006 step 7 — "do not guess"):
# conversation.py:67, saga.py:67, dlq.py:97, job_store.py:87,
# audit.py:114, outbox.py:141, inbox.py:141, deduplication.py:111
# (varco_dedup_log, NOT varco_dedup as informally suggested in the plan text),
# encryption_store.py:83.
EXPECTED_FRAMEWORK_TABLE_NAMES = frozenset(
    {
        "varco_outbox",
        "varco_inbox",
        "varco_jobs",
        "varco_sagas",
        "varco_conversation_turns",
        "varco_dedup_log",
        "varco_audit_log",
        "varco_dead_letters",
        "varco_encryption_keys",
        # Plan 007, Phase 4, step 2 — the tenant catalog, the tenth
        # framework table (varco_sa/tenancy/models.py).
        "varco_tenants",
        # Plan 029 / D1b — the eleventh framework table
        # (varco_sa/idempotency.py).
        "varco_idempotency",
        # Plan 031 / D4a — the twelfth framework table (varco_sa/webhook.py).
        "webhook_subscriptions",
        # Plan 032 / D6, Step 10 — the thirteenth framework table
        # (varco_sa/schedule.py).
        "schedules",
    }
)

# Bases that are explicitly NOT framework tables — excluded per step 6.
_EXCLUDED_METADATA_OWNERS = {"BaseDatabaseModel"}


def _walk_module_level_metadata_objects() -> list[MetaData]:
    """Collect every module-level MetaData / DeclarativeBase.metadata in varco_sa."""
    collected: list[MetaData] = []
    seen_ids: set[int] = set()

    for _finder, name, _ispkg in pkgutil.walk_packages(varco_sa.__path__, prefix="varco_sa."):
        if "tests" in name:
            continue
        module = importlib.import_module(name)
        for attr_name in dir(module):
            if attr_name.startswith("_"):
                continue
            attr = getattr(module, attr_name)
            if isinstance(attr, MetaData) and id(attr) not in seen_ids:
                seen_ids.add(id(attr))
                collected.append(attr)
            elif (
                isinstance(attr, type)
                and issubclass(attr, DeclarativeBase)
                and attr is not DeclarativeBase
                and attr.__name__ not in _EXCLUDED_METADATA_OWNERS
                and id(attr.metadata) not in seen_ids
            ):
                seen_ids.add(id(attr.metadata))
                collected.append(attr.metadata)

    return collected


async def test_every_framework_table_is_a_subset_of_framework_metadata() -> None:
    from varco_sa.metadata import framework_metadata

    aggregated = framework_metadata()
    all_framework_table_names = set(aggregated.tables.keys())

    for md in _walk_module_level_metadata_objects():
        for table_name in md.tables:
            assert table_name in all_framework_table_names, (
                f"table {table_name!r} is not registered in framework_metadata() "
                "— add register_framework_metadata() at the owning module's import time"
            )


async def test_framework_table_names_matches_expected_literal_set() -> None:
    from varco_sa.metadata import framework_table_names

    assert framework_table_names() == EXPECTED_FRAMEWORK_TABLE_NAMES


async def test_framework_metadata_tables_match_expected_literal_set() -> None:
    from varco_sa.metadata import framework_metadata

    assert set(framework_metadata().tables.keys()) == EXPECTED_FRAMEWORK_TABLE_NAMES


async def test_register_framework_metadata_adds_a_new_metadata_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import varco_sa.metadata as metadata_module
    from varco_sa.metadata import framework_metadata, register_framework_metadata

    # Plan 059 / Step 3: the registry is process-global — register into a
    # throwaway copy so the two exact-name guards above stay green when
    # pytest-randomly runs them after this test.
    monkeypatch.setattr(
        metadata_module, "_FRAMEWORK_METADATA", dict(metadata_module._FRAMEWORK_METADATA)
    )
    md = MetaData()
    from sqlalchemy import Column, Integer, Table

    Table("a_brand_new_framework_table", md, Column("id", Integer, primary_key=True))

    register_framework_metadata("test_module_fixture", md)

    assert "a_brand_new_framework_table" in framework_metadata().tables


async def test_encryption_metadata_alias_is_exported() -> None:
    # Source correction 2 — encryption_store.py had no public alias at all.
    from varco_sa import encryption_metadata

    assert "varco_encryption_keys" in encryption_metadata.tables


async def test_audit_metadata_alias_is_exported() -> None:
    from varco_sa import audit_metadata

    assert "varco_audit_log" in audit_metadata.tables


async def test_dead_letters_metadata_alias_is_exported() -> None:
    from varco_sa import dead_letters_metadata

    assert "varco_dead_letters" in dead_letters_metadata.tables


async def test_get_target_metadata_include_framework_merges_framework_tables() -> None:
    from varco_sa.alembic_helpers import get_target_metadata

    md = get_target_metadata(include_framework=True)

    assert EXPECTED_FRAMEWORK_TABLE_NAMES.issubset(set(md.tables.keys()))


async def test_get_target_metadata_default_excludes_framework_tables() -> None:
    from varco_sa.alembic_helpers import get_target_metadata

    md = get_target_metadata()

    assert set(md.tables.keys()).isdisjoint(EXPECTED_FRAMEWORK_TABLE_NAMES)
