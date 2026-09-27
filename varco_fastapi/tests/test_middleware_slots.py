"""
Plan 049 / Step 4 — `EdgeSlot`/`MiddlewareSpec`/`sort_specs`/`HttpContributor`
(§D-http), Docker-free.

RED MODE: `varco_fastapi.middleware.slots` and `varco_fastapi.modules.base` do
not exist yet (Step 5 adds them) — every test below is expected to fail with
`ImportError`/`ModuleNotFoundError`.
"""

from __future__ import annotations

import pytest
from varco_core.di.module import VarcoModule
from varco_fastapi.middleware.slots import EdgeSlot, MiddlewareSpec, sort_specs
from varco_fastapi.modules.base import HttpContributor

# The outermost-first sequence as documented in
# varco_fastapi/middleware/__init__.py:18-37 and §D-order's table.
EXPECTED_SLOT_NAMES = (
    "CORS",
    "SECURITY_HEADERS",
    "EXTRA",
    "ERROR",
    "BODY_LIMIT",
    "LOGGING",
    "TRACING",
    "METRICS",
    "RATE_LIMIT_PRE_AUTH",
    "REQUEST_CONTEXT",
    "RATE_LIMIT_POST_AUTH",
    "LOCALIZATION",
    "IDEMPOTENCY",
    "PROFILING",
)


class TestEdgeSlotOrdering:
    async def test_member_list_matches_literal_documented_order(self) -> None:
        actual_names = tuple(m.name for m in EdgeSlot)

        assert actual_names == EXPECTED_SLOT_NAMES

    async def test_values_are_strictly_increasing_in_declared_order(self) -> None:
        values = [m.value for m in EdgeSlot]

        assert values == sorted(values)
        assert len(values) == len(set(values))


class TestMiddlewareSpec:
    async def test_spec_is_frozen(self) -> None:
        spec = MiddlewareSpec(slot=EdgeSlot.CORS, target=object, kwargs={})

        with pytest.raises(Exception):
            spec.slot = EdgeSlot.LOGGING  # type: ignore[misc]

    async def test_spec_is_hashable(self) -> None:
        spec = MiddlewareSpec(slot=EdgeSlot.CORS, target=object, kwargs={})

        hash(spec)  # must not raise


class TestSortSpecs:
    async def test_sorts_outermost_first_by_slot(self) -> None:
        pre_auth = MiddlewareSpec(slot=EdgeSlot.RATE_LIMIT_PRE_AUTH, target=object, kwargs={})
        cors = MiddlewareSpec(slot=EdgeSlot.CORS, target=object, kwargs={})
        logging_spec = MiddlewareSpec(slot=EdgeSlot.LOGGING, target=object, kwargs={})

        result = sort_specs([pre_auth, logging_spec, cors])

        assert result == [cors, logging_spec, pre_auth]

    async def test_stable_within_one_slot(self) -> None:
        first = MiddlewareSpec(slot=EdgeSlot.EXTRA, target="first", kwargs={})
        second = MiddlewareSpec(slot=EdgeSlot.EXTRA, target="second", kwargs={})

        result = sort_specs([first, second])

        assert result == [first, second]


class TestHttpContributorProtocol:
    async def test_is_runtime_checkable(self) -> None:
        assert getattr(HttpContributor, "_is_runtime_protocol", False) or isinstance(
            HttpContributor, type
        )
        # runtime_checkable Protocols support isinstance()
        isinstance(object(), HttpContributor)  # must not raise TypeError

    async def test_bare_varco_module_without_http_methods_is_not_a_contributor(self) -> None:
        class _PlainModule(VarcoModule):
            name = "plain"

            def _register(self, container: object) -> None:  # type: ignore[override]
                pass

        assert isinstance(_PlainModule(), HttpContributor) is False

    async def test_module_with_middleware_and_configure_app_is_a_contributor(self) -> None:
        class _HttpModule(VarcoModule):
            name = "http"

            def _register(self, container: object) -> None:  # type: ignore[override]
                pass

            def middleware(self, container: object) -> list[MiddlewareSpec]:
                return []

            def configure_app(self, app: object, container: object) -> None:
                pass

        assert isinstance(_HttpModule(), HttpContributor) is True
