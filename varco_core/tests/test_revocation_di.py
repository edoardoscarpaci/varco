"""
Unit tests for varco_core.revocation.di (Plan 034 / S13a, Step 19, §D-S13-di).

Mirrors varco_core.flags.di's enable_feature_flags precedent:
NullTokenRevocationStore is bound by default, enable_token_revocation()
is the only way to opt into InMemoryTokenRevocationStore.
"""

from __future__ import annotations

from providify import DIContainer


async def test_null_token_revocation_store_bound_by_default() -> None:
    from varco_core.revocation import AbstractTokenRevocationStore
    from varco_core.revocation.null import NullTokenRevocationStore

    container = DIContainer()
    container.scan("varco_core.revocation", recursive=True)
    store = await container.aget(AbstractTokenRevocationStore)
    assert isinstance(store, NullTokenRevocationStore)
    await container.ashutdown()


async def test_enable_token_revocation_swaps_the_default() -> None:
    from varco_core.revocation import AbstractTokenRevocationStore
    from varco_core.revocation.di import enable_token_revocation
    from varco_core.revocation.memory import InMemoryTokenRevocationStore
    from varco_core.revocation.null import NullTokenRevocationStore

    container = DIContainer()
    container.scan("varco_core.revocation", recursive=True)

    before = await container.aget(AbstractTokenRevocationStore)
    assert isinstance(before, NullTokenRevocationStore)

    enable_token_revocation(container)
    after = await container.aget(AbstractTokenRevocationStore)
    assert isinstance(after, InMemoryTokenRevocationStore)
    await container.ashutdown()
