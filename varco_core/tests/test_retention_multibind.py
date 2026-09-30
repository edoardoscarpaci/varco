"""
Plan 051 (DI-5) / §D-retention — RED tests: ``RetentionPolicy`` becomes a
``@Multibound`` collection seam; ``RetentionRegistry`` is constructed from
an iterable of policies (``register()`` is removed); ``bind_retention_registry``
/``varco_core.retention.di`` are deleted.

Expected to fail until Step 6 lands:
- ``RetentionRegistry()`` currently takes no constructor argument and has a
  public ``register()`` method — the new shape is the opposite.
- ``varco_core.retention.di`` still exists today (deleted by this plan).
"""

from __future__ import annotations

import pytest


def test_retention_policy_is_multibound_seam() -> None:
    from varco_core.retention.policy import RetentionPolicy

    assert getattr(RetentionPolicy, "__di_multibound__", None) is not None


def test_retention_registry_constructed_from_iterable_of_policies() -> None:
    from datetime import timedelta

    from varco_core.retention.policy import RetentionPolicy, RetentionRegistry
    from varco_core.retention.targets import DlqRetentionTarget

    class _FakeDlq:
        supports_delete_where = True

        async def delete_where(self, **kwargs):  # pragma: no cover - not exercised
            return 0

    target = DlqRetentionTarget(dlq=_FakeDlq(), acknowledge_dead_letter_deletion=True)
    policy = RetentionPolicy(
        name="p1",
        target=target,
        cron_expr="0 3 * * *",
        timezone="UTC",
        dry_run=True,
        older_than=timedelta(days=30),
    )

    registry = RetentionRegistry([policy])  # new ctor signature
    assert registry.get("p1") is policy


def test_retention_registry_has_no_public_register_method() -> None:
    from varco_core.retention.policy import RetentionRegistry

    registry = RetentionRegistry([])
    assert not hasattr(registry, "register")


def test_bind_retention_registry_module_removed() -> None:
    with pytest.raises(ImportError):
        from varco_core.retention.di import bind_retention_registry  # noqa: F401,PLC0415


async def test_collect_retention_policy_includes_a_provider_contribution() -> None:
    from datetime import timedelta

    from providify import Configuration, DIContainer, Provider
    from varco_core.di.collect import collect
    from varco_core.retention.policy import RetentionPolicy
    from varco_core.retention.targets import DlqRetentionTarget

    class _FakeDlq:
        supports_delete_where = True

        async def delete_where(self, **kwargs):  # pragma: no cover
            return 0

    @Configuration
    class _PolicyConfig:
        # returns= explicit override: `from __future__ import annotations` +
        # a function-scoped `import RetentionPolicy` above means the bare
        # `-> RetentionPolicy` return annotation is an unresolvable string at
        # this module's globals (RetentionPolicy was never bound there) —
        # see providify/binding.py's ProviderBinding docstring on `returns=`
        # precedence.
        @Provider(singleton=True, returns=RetentionPolicy)
        def policy(self) -> RetentionPolicy:
            return RetentionPolicy(
                name="plugin-policy",
                target=DlqRetentionTarget(dlq=_FakeDlq(), acknowledge_dead_letter_deletion=True),
                cron_expr="0 3 * * *",
                timezone="UTC",
                dry_run=True,
                older_than=timedelta(days=30),
            )

    container = DIContainer()
    container.install(_PolicyConfig)

    result = collect(container, RetentionPolicy)
    assert len(result) == 1
    assert result[0].name == "plugin-policy"
