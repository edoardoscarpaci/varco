"""
varco_fastapi.middleware.slots
=================================
``EdgeSlot``/``MiddlewareSpec``/``sort_specs`` — Plan 049 / DI-1, §D-http.

``EdgeSlot`` **is** the normative middleware order table, as code — the
same outermost-first sequence ``varco_fastapi.middleware``'s module
docstring documents in prose, pinned together by
``test_middleware_slots.py``'s literal tuple assertion so the two can never
silently drift apart again.

A ``VarcoModule`` that also implements ``HttpContributor`` (see
``varco_fastapi.modules.base``) names a slot rather than inventing a
position: ``create_varco_app`` collects every module's
``middleware(container)`` result, sorts the combined list by
``MiddlewareSpec.slot`` (stable — two entries in the same slot keep module
list order), and installs the result once via
``varco_fastapi.middleware.install_middleware_stack``.

Thread safety:  ✅ ``EdgeSlot``/``MiddlewareSpec`` are immutable; ``sort_specs``
                    is a pure function.
Async safety:   ✅ No I/O anywhere in this module.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any


class EdgeSlot(IntEnum):
    """
    The outermost-first middleware position table, as code (§D-http).

    Values are deliberately spaced by ten — room for a future slot to be
    inserted between two existing ones without renumbering everything.
    """

    CORS = 10
    SECURITY_HEADERS = 20
    EXTRA = 30
    ERROR = 40
    BODY_LIMIT = 50
    LOGGING = 60
    TRACING = 70
    METRICS = 80
    RATE_LIMIT_PRE_AUTH = 90
    REQUEST_CONTEXT = 100
    RATE_LIMIT_POST_AUTH = 110
    LOCALIZATION = 120
    IDEMPOTENCY = 130
    PROFILING = 140


@dataclass(frozen=True)
class MiddlewareSpec:
    """
    One middleware a ``VarcoModule`` wants installed, at a named
    ``EdgeSlot``.

    Args:
        slot: The ``EdgeSlot`` position this middleware occupies.
        target: A middleware class (``app.add_middleware(target,
            **kwargs)``) or a callable installer (``target(app,
            **kwargs)``) — the same ``MiddlewareEntry`` shape
            ``install_middleware_stack`` already accepts.
        kwargs: Keyword arguments forwarded to *target*.

    Edge cases:
        ``kwargs`` is excluded from the generated ``__hash__`` (a ``dict``
        is unhashable) but still participates in equality — two specs with
        the same ``(slot, target)`` and different ``kwargs`` hash equal but
        compare unequal, same as any dataclass field marked ``hash=False``.
    """

    slot: EdgeSlot
    target: type | Callable[..., None]
    kwargs: Mapping[str, Any] = field(default_factory=dict, hash=False)


def sort_specs(specs: Sequence[MiddlewareSpec]) -> list[MiddlewareSpec]:
    """
    Sort *specs* outermost-first by ``slot``, stable within one slot.

    Args:
        specs: The combined ``MiddlewareSpec`` list collected from every
            active module's ``middleware(container)``.

    Returns:
        A new, sorted list — Python's ``sorted`` is stable, so two specs
        in the same slot keep their relative (module-list) order.
    """
    return sorted(specs, key=lambda s: s.slot)


__all__ = ["EdgeSlot", "MiddlewareSpec", "sort_specs"]
