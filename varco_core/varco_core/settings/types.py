"""
varco_core.settings.types
==========================
Shared field types for ``VarcoSettings`` subclasses (Plan 048 / DI-6).

``CommaSeparated`` is the one shared annotation every comma-separated
``VARCO_*_ORIGINS``/``VARCO_*_METHODS``-style field should use, so that
pydantic-settings' JSON-decoding of ``list``/``tuple`` annotations never
fires for them (``VARCO_CORS_ORIGINS=https://a,https://b`` must keep working
without every reader hand-rolling a ``BeforeValidator``).

DESIGN: ``NoDecode`` + a shared ``BeforeValidator`` over a bare ``tuple[str, ...]``
    ✅ pydantic-settings' default behaviour for a ``list``/``tuple``-annotated
       field is to JSON-decode the env string first
       (``pydantic_settings.sources.providers.env``) — ``VARCO_CORS_ORIGINS=
       https://a,https://b`` would fail with a JSON decode error before this
       annotation existed. ``NoDecode`` (`pydantic_settings.NoDecode`)
       disables that JSON pass entirely, handing the raw string to our own
       validator instead.
    ✅ One annotation, reused by every converted class (Step 5) — no class
       hand-rolls its own comma-split validator, so the splitting rule
       (strip whitespace around each item) is uniform across the whole
       ``VARCO_*`` surface.
    ❌ A field typed ``CommaSeparated`` can never receive a JSON-array string
       from the environment — acceptable, since no in-tree class ever
       documented that shape, and comma-separated is the incumbent
       convention (``CORSConfig.from_env()`` today).

Thread safety:  N/A — a type alias and a pure function, no shared state.
Async safety:   N/A — no I/O.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import BeforeValidator
from pydantic_settings import NoDecode


def _split_csv(value: object) -> tuple[str, ...]:
    """
    Split a comma-separated string into a tuple of stripped, non-empty items.

    Args:
        value: The raw value being validated. A ``str`` is split on ``,``;
            any other value (e.g. already a ``tuple``/``list``, as happens
            when a caller passes ``origins=("a", "b")`` directly via
            keyword args rather than through the environment) is returned
            as a plain ``tuple`` unchanged, item-for-item.

    Returns:
        A tuple of stripped strings. An empty or whitespace-only string
        yields ``()``.

    Edge cases:
        - ``"a, b ,c"`` -> ``("a", "b", "c")`` — surrounding whitespace per
          item is stripped, matching ``CORSConfig.from_env()``'s incumbent
          behaviour.
        - ``""`` -> ``()``, never ``("",)``.
    """
    if isinstance(value, str):
        return tuple(item.strip() for item in value.split(",") if item.strip())
    return tuple(value)  # type: ignore[arg-type]


#: A comma-separated field type: ``VARCO_X_ORIGINS=a,b,c`` -> ``("a", "b", "c")``.
#: See the module DESIGN block for why ``NoDecode`` is required alongside the
#: validator (otherwise pydantic-settings' own JSON pre-decode raises first).
CommaSeparated = Annotated[tuple[str, ...], NoDecode, BeforeValidator(_split_csv)]


__all__ = ["CommaSeparated"]
