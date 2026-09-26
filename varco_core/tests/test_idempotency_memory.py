"""
Plan 047 / Step 4 — `InMemoryIdempotencyStore` becomes a scanned
`@Fallback @Singleton(qualifier="in_memory")` default (§D-vocab's
`idempotency` row).

Premise (2), corrected in the plan's header: today `InMemoryIdempotencyStore`
carries no `@Singleton`/`@Provider` at all — the middleware takes `store=`
explicitly. This plan makes it resolvable by default.

RED MODE: `InMemoryIdempotencyStore` is not decorated yet — every
resolution test here is expected to fail (`LookupError` from providify, not
an import error, since the class itself already exists).
"""

from __future__ import annotations

from providify import DIContainer


def test_in_memory_idempotency_store_is_resolvable_by_default_with_qualifier() -> None:
    from varco_core.idempotency.base import AbstractIdempotencyStore
    from varco_core.idempotency.memory import InMemoryIdempotencyStore

    container = DIContainer()
    container.scan("varco_core", recursive=True)

    resolved = container.get(AbstractIdempotencyStore, qualifier="in_memory")
    assert isinstance(resolved, InMemoryIdempotencyStore)


def test_in_memory_idempotency_store_binding_is_a_fallback() -> None:
    from varco_core.idempotency.base import AbstractIdempotencyStore

    container = DIContainer()
    container.scan("varco_core", recursive=True)

    binding = container.get_binding(AbstractIdempotencyStore, qualifier="in_memory")
    assert binding.fallback is True


def test_in_memory_idempotency_store_unqualified_resolution_also_succeeds() -> None:
    # Unset VARCO_IDEMPOTENCY_BACKEND → the in-process default serves the
    # unqualified request too (§D-unset).
    from varco_core.idempotency.base import AbstractIdempotencyStore
    from varco_core.idempotency.memory import InMemoryIdempotencyStore

    container = DIContainer()
    container.scan("varco_core", recursive=True)

    resolved = container.get(AbstractIdempotencyStore)
    assert isinstance(resolved, InMemoryIdempotencyStore)
