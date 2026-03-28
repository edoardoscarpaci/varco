"""
varco_beanie.index_guard
========================
Startup index drift detector for Beanie (MongoDB) + varco.

Compares the indexes expected from ``DomainModel`` metadata (``FieldHint``,
``UniqueConstraint``, composite PK hints) against the indexes that actually
exist in the MongoDB collection, and reports any that are missing.

Typical usage — call once at application startup before serving requests::

    from varco_beanie.index_guard import BeanieIndexGuard

    guard = BeanieIndexGuard(User, Post, Tag)
    await guard.check(db)   # raises IndexDrift if any expected index is missing

    # Or non-raising variant for health / diagnostic endpoints:
    drift = await guard.report(db)
    if drift.has_drift:
        logger.error(drift.format())

DESIGN: parallel with SchemaGuard in varco_sa
    ✅ Users who know SchemaGuard already understand BeanieIndexGuard — same
       check() / report() API, same frozen report dataclass, same exception type.
    ✅ startup-time detection is far better than discovering missing indexes
       in production through slow queries or uniqueness violations.
    ❌ MongoDB doesn't enforce a schema — indexes can drift silently after the
       app starts (e.g. manual drops).  Guard only runs at startup.

DESIGN: match by key tuple + unique flag, NOT by index name
    ✅ Beanie auto-generates index names like ``"email_1"`` that may differ
       from the ``UniqueConstraint.name`` in the domain model.
    ✅ Comparison is backend-agnostic — user-assigned names are optional and
       should not be required for drift detection to work.
    ❌ Two logically identical indexes with different names appear as matching
       — e.g. a manually-created ``my_email_idx`` is treated as satisfying
       ``FieldHint(index=True)`` on ``email``.  This is the desired behaviour.

Thread safety:  ✅ Stateless after construction.
Async safety:   ✅ check() and report() are async def; index_information() is awaited.

📚 Docs
- 🔍 https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.index_information
  Collection.index_information() — returns dict of actual indexes
- 🔍 https://pymongo.readthedocs.io/en/stable/api/pymongo/operations.html#pymongo.operations.IndexModel
  IndexModel — compound index specification
- 🐍 https://docs.python.org/3/library/dataclasses.html#frozen-instances
  frozen=True — immutable report object
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from varco_core.meta import MetaReader, UniqueConstraint
from varco_core.model import DomainModel

if TYPE_CHECKING:
    # Motor AsyncIOMotorDatabase — only needed for type hints; avoids importing
    # motor in environments that swap in a test double.
    from motor.motor_asyncio import AsyncIOMotorDatabase  # type: ignore[import-untyped]

_logger = logging.getLogger(__name__)


# ── Index specification types ─────────────────────────────────────────────────


@dataclass(frozen=True)
class _ExpectedIndex:
    """
    Internal value object representing one expected MongoDB index.

    Matches against actual ``index_information()`` entries by comparing the
    ``key_fields`` tuple and ``unique`` flag — NOT by name.

    Attributes:
        collection: MongoDB collection name (from ``Meta.table``).
        key_fields: Tuple of field names that form the index key, in order.
                    Single-field index: ``("email",)``.
                    Compound index:     ``("user_id", "role_id")``.
        unique:     Whether the index enforces uniqueness.
        label:      Human-readable description for drift reports.
    """

    collection: str
    key_fields: tuple[str, ...]
    unique: bool
    label: str


# ── Drift report ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class IndexDriftReport:
    """
    Immutable snapshot of index drift between expected and actual MongoDB state.

    ``missing_indexes`` are *blocking* — a missing unique index means the
    uniqueness guarantee is absent and writes may succeed that should fail.
    A missing non-unique index means query performance is degraded.

    ``unexpected_indexes`` are *informational* — extra indexes don't break
    correctness; they may cost write performance or be legacy artifacts.

    Attributes:
        missing_indexes:    ``{collection: [label, ...]}`` — expected indexes
                            not found in the live collection.  Blocking.
        unexpected_indexes: ``{collection: [name, ...]}`` — indexes in the live
                            collection that were not expected.  Non-blocking.

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ No mutable state.

    Edge cases:
        - ``has_drift`` returns ``True`` only for missing indexes (blocking).
          Extra indexes alone do NOT set ``has_drift``.
        - Empty dicts → clean state.
    """

    missing_indexes: dict[str, list[str]] = field(default_factory=dict)
    """Blocking: ``{collection: [missing_index_label, ...]}``."""

    unexpected_indexes: dict[str, list[str]] = field(default_factory=dict)
    """Informational: ``{collection: [extra_index_name, ...]}``."""

    @property
    def has_drift(self) -> bool:
        """
        Return ``True`` if any expected index is missing (blocking drift only).

        Extra indexes alone do not constitute drift — they are warnings.

        Returns:
            ``True`` if ``missing_indexes`` is non-empty.
        """
        return bool(self.missing_indexes)

    def format(self) -> str:
        """
        Return a human-readable multi-line summary of all detected drift.

        Returns:
            A formatted string suitable for logging or console output.
            Empty string if no drift or unexpected indexes were found.

        Edge cases:
            - Called on a clean report → returns "No index drift detected."
        """
        if not self.has_drift and not self.unexpected_indexes:
            return "No index drift detected."

        lines: list[str] = []

        if self.missing_indexes:
            lines.append("MISSING INDEXES (blocking — create these indexes):")
            for coll, labels in sorted(self.missing_indexes.items()):
                for label in labels:
                    lines.append(f"  {coll}: {label}")

        if self.unexpected_indexes:
            lines.append("UNEXPECTED INDEXES (informational — not in domain model):")
            for coll, names in sorted(self.unexpected_indexes.items()):
                for name in names:
                    lines.append(f"  {coll}: {name}")

        return "\n".join(lines)

    def __repr__(self) -> str:
        return (
            f"IndexDriftReport("
            f"missing={sum(len(v) for v in self.missing_indexes.values())}, "
            f"unexpected={sum(len(v) for v in self.unexpected_indexes.values())})"
        )


# ── IndexDrift exception ──────────────────────────────────────────────────────


class IndexDrift(Exception):
    """
    Raised by ``BeanieIndexGuard.check()`` when expected indexes are missing.

    Attributes:
        report: The full ``IndexDriftReport`` with all missing and unexpected indexes.

    Edge cases:
        - Only raised for MISSING indexes — unexpected indexes trigger a warning
          log but do not raise.
    """

    def __init__(self, report: IndexDriftReport) -> None:
        self.report = report
        super().__init__(f"MongoDB index drift detected:\n{report.format()}")


# ── BeanieIndexGuard ──────────────────────────────────────────────────────────


class BeanieIndexGuard:
    """
    Compares expected MongoDB indexes (from domain model metadata) against the
    live collection indexes at startup.

    Build the guard with all entity classes, then call ``check()`` or
    ``report()`` once during app initialization::

        guard = BeanieIndexGuard(User, Post, Tag)
        await guard.check(db)   # raises IndexDrift on missing indexes

    Expected indexes are derived from ``DomainModel`` metadata via
    ``MetaReader.read()``:

    +----------------------------------+-----------------------------------+
    | Metadata                         | Expected index                    |
    +==================================+===================================+
    | ``FieldHint(unique=True)``       | single-field unique index         |
    +----------------------------------+-----------------------------------+
    | ``FieldHint(index=True)``        | single-field non-unique index     |
    +----------------------------------+-----------------------------------+
    | ``UniqueConstraint(fields=...)`` | compound unique index             |
    +----------------------------------+-----------------------------------+
    | PK field (single or composite)   | ``_id_`` always expected (skipped)|
    +----------------------------------+-----------------------------------+

    Thread safety:  ✅ Stateless after construction.
    Async safety:   ✅ check() and report() are async def.

    Edge cases:
        - Indexes are matched by key-fields + unique flag, NOT by name.
          A manually-created index covering the same fields is treated as
          satisfying the expectation even if its name differs.
        - The ``_id_`` index is always present in MongoDB and is skipped
          during comparison to avoid false positives.
        - ``CheckConstraint`` is ignored — MongoDB has no SQL CHECK equivalent.
        - ``ForeignKey`` hints are not indexed automatically — add
          ``FieldHint(index=True)`` explicitly if you need a FK index.
    """

    def __init__(self, *entity_classes: type[DomainModel]) -> None:
        """
        Args:
            *entity_classes: One or more ``DomainModel`` subclasses to inspect.

        Edge cases:
            - Empty ``entity_classes`` is allowed — ``check()`` / ``report()``
              will return a clean report immediately.
        """
        self._entity_classes = entity_classes

    # ── Public API ─────────────────────────────────────────────────────────────

    async def check(self, db: AsyncIOMotorDatabase) -> None:
        """
        Check all registered collections for missing indexes.

        Raises:
            IndexDrift: If any expected index is absent from the live collection.

        Args:
            db: The live Motor database to inspect.

        Edge cases:
            - Does not raise for unexpected (extra) indexes — those are only
              logged as warnings.
        """
        rpt = await self.report(db)
        if rpt.has_drift:
            raise IndexDrift(rpt)
        if rpt.unexpected_indexes:
            for coll, names in rpt.unexpected_indexes.items():
                _logger.warning(
                    "BeanieIndexGuard: %s has unexpected indexes: %s",
                    coll,
                    names,
                )

    async def report(self, db: AsyncIOMotorDatabase) -> IndexDriftReport:
        """
        Compare expected indexes against live collection indexes.

        Returns an ``IndexDriftReport`` without raising, even if drift exists.
        Use this for health-check endpoints or diagnostic tooling.

        Args:
            db: The live Motor database to inspect.

        Returns:
            An ``IndexDriftReport`` with missing and unexpected indexes.

        Async safety:  ✅ Awaits index_information() per collection.
        """
        if not self._entity_classes:
            return IndexDriftReport()

        expected = self._build_expected_indexes()
        return await self._compare(db, expected)

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _build_expected_indexes(self) -> list[_ExpectedIndex]:
        """
        Derive all expected indexes from domain model metadata.

        Iterates all registered entity classes, reads their ``ParsedMeta``,
        and builds ``_ExpectedIndex`` entries for each relevant field hint
        and table constraint.

        Returns:
            A flat list of ``_ExpectedIndex`` instances — one per expected index.

        Edge cases:
            - Composite PK fields are marked with ``unique=True`` in Beanie
              (since MongoDB uses ``_id`` for single PKs and Indexed(unique=True)
              for composite fields).  However, a single ``_id_`` index is ALWAYS
              present and is excluded from expectations here to avoid double-reporting.
            - ``ForeignKey`` hints without ``FieldHint(index=True)`` are NOT indexed
              — this is intentional.  FK columns in MongoDB are just regular fields.
        """
        indexes: list[_ExpectedIndex] = []

        for entity_cls in self._entity_classes:
            meta = MetaReader.read(entity_cls)
            collection = meta.table

            # ── Single-field indexes from FieldHint ────────────────────────────
            for field_name, hint in meta.fields.items():
                if hint is None:
                    continue

                if hint.unique:
                    # unique=True → Beanie wraps with Indexed(type, unique=True)
                    # → MongoDB single-field unique index
                    indexes.append(
                        _ExpectedIndex(
                            collection=collection,
                            key_fields=(field_name,),
                            unique=True,
                            label=f"unique index on {field_name!r}",
                        )
                    )
                elif hint.index:
                    # index=True (no unique) → Indexed(type) → plain index
                    indexes.append(
                        _ExpectedIndex(
                            collection=collection,
                            key_fields=(field_name,),
                            unique=False,
                            label=f"index on {field_name!r}",
                        )
                    )

            # ── Composite PK fields ────────────────────────────────────────────
            # Composite PK fields are stored as regular Beanie fields with
            # Indexed(unique=True) — they are not the _id field.
            for cpk_field in meta.composite_pk_fields:
                # Only add if not already covered by a FieldHint(unique=True)
                # to avoid duplicate expectations for the same field.
                if cpk_field not in meta.fields or not (
                    meta.fields.get(cpk_field) and meta.fields[cpk_field].unique  # type: ignore[union-attr]
                ):
                    indexes.append(
                        _ExpectedIndex(
                            collection=collection,
                            key_fields=(cpk_field,),
                            unique=True,
                            label=f"composite PK unique index on {cpk_field!r}",
                        )
                    )

            # ── Table-level UniqueConstraint (compound indexes) ─────────────────
            for constraint in meta.constraints:
                if isinstance(constraint, UniqueConstraint):
                    indexes.append(
                        _ExpectedIndex(
                            collection=collection,
                            key_fields=tuple(constraint.fields),
                            unique=True,
                            label=(
                                f"compound unique index on "
                                f"({', '.join(repr(f) for f in constraint.fields)})"
                                + (f" [{constraint.name}]" if constraint.name else "")
                            ),
                        )
                    )
                # CheckConstraint is ignored — MongoDB has no SQL CHECK equivalent.

        return indexes

    async def _compare(
        self,
        db: AsyncIOMotorDatabase,
        expected: list[_ExpectedIndex],
    ) -> IndexDriftReport:
        """
        Compare ``expected`` against live ``index_information()`` for each collection.

        Args:
            db:       The live Motor database.
            expected: Flat list of ``_ExpectedIndex`` objects to verify.

        Returns:
            ``IndexDriftReport`` with any missing or unexpected indexes.

        DESIGN: match by (key_fields_tuple, unique) NOT by name
            ✅ Beanie auto-names indexes ("email_1") — user names are advisory.
            ✅ A manually-created index satisfies the expectation even with
               a different name — no false positives.
            ❌ Two indexes on the same fields (e.g. one unique, one not) are
               technically different — we distinguish them by the unique flag.
        """
        # Group expected indexes by collection for efficient per-collection lookup.
        by_collection: dict[str, list[_ExpectedIndex]] = {}
        for idx in expected:
            by_collection.setdefault(idx.collection, []).append(idx)

        missing_indexes: dict[str, list[str]] = {}
        unexpected_indexes: dict[str, list[str]] = {}

        for collection_name, expected_for_coll in by_collection.items():
            # Fetch all actual indexes from the live collection.
            actual_raw: dict[str, Any] = await db[collection_name].index_information()

            # Build a set of (key_fields_tuple, unique) from actual indexes for
            # O(1) lookup.  Skip the _id_ index — it is always present and never
            # needs to be in the domain model.
            actual_signatures: set[tuple[tuple[str, ...], bool]] = set()
            for idx_name, idx_info in actual_raw.items():
                if idx_name == "_id_":
                    # _id_ is always present — skip to avoid false positives.
                    continue
                key_fields = tuple(f for f, _ in idx_info.get("key", []))
                is_unique = bool(idx_info.get("unique", False))
                actual_signatures.add((key_fields, is_unique))

            # Check each expected index against the actual signatures.
            for exp in expected_for_coll:
                sig = (exp.key_fields, exp.unique)
                if sig not in actual_signatures:
                    missing_indexes.setdefault(collection_name, []).append(exp.label)

            # Identify unexpected indexes — actual indexes not in any expectation.
            expected_sigs: set[tuple[tuple[str, ...], bool]] = {
                (e.key_fields, e.unique) for e in expected_for_coll
            }
            for idx_name, idx_info in actual_raw.items():
                if idx_name == "_id_":
                    continue
                key_fields = tuple(f for f, _ in idx_info.get("key", []))
                is_unique = bool(idx_info.get("unique", False))
                if (key_fields, is_unique) not in expected_sigs:
                    unexpected_indexes.setdefault(collection_name, []).append(idx_name)

        return IndexDriftReport(
            missing_indexes=missing_indexes,
            unexpected_indexes=unexpected_indexes,
        )

    def __repr__(self) -> str:
        names = ", ".join(c.__name__ for c in self._entity_classes)
        return f"BeanieIndexGuard({names})"


__all__ = [
    "BeanieIndexGuard",
    "IndexDrift",
    "IndexDriftReport",
]
