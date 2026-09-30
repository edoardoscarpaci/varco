"""
varco_fastapi.router._di
===========================
``_resolve_optional(container, cls)`` — the P-INJ shim (Plan 050 / §D-asks,
`design/upstream-gaps/providify-provider-inject-override.md`) centralising
the "try to resolve a runtime-only class from the container, fall back to
``None`` on any failure" pattern ``bind_mcp_adapter``/``bind_skill_adapter``
each hand-rolled at their own call sites (``mcp.py``, ``skill.py``).

Why this cannot be a declared ``Inject[...]`` parameter
--------------------------------------------------------
Every OTHER Plan 050 injection fix in this repo (``varco_ws/di.py``'s
``bus: Inject[AbstractEventBus]``) works by moving an import to module
scope so a PEP-563 (``from __future__ import annotations``) deferred
annotation string resolves against the *defining module's* globals
(``providify/_annotations.py:180-235``). That trick only works for a type
that is itself a stable, module-importable name.

``client_cls``/``job_runner_cls``/``job_store_cls``/``conversation_store_cls``
here are **runtime values** — arbitrary classes passed as ordinary
constructor arguments to ``bind_mcp_adapter``/``bind_skill_adapter`` at
*call time*, not module-level imports this file could ever name in
advance. There is no annotation to write, deferred or otherwise — a
``Provider`` factory's parameter types are fixed at *decoration* time
(``providify/decorator/scope.py:497-511`` has no ``inject=`` parameter-
override kwarg, the exact gap this shim papers over — filed as **P-INJ**,
`design/upstream-gaps/providify-provider-inject-override.md`).

DESIGN: one shared function over four copy-pasted try/except blocks
    ✅ A single, named, documented, *deletable* place to remove once
       providify ships ``provide(fn, inject=...)`` / ``@Provider(inject=...)``
       — the P-INJ report names this exact function as the thing to delete.
    ✅ Uniform failure shape: swallow any resolution error (unbound
       interface, ambiguous binding, whatever) and return ``None`` — the
       call site decides what "no adapter available" means (a warning log,
       a bare fallback construction, or silently disabled functionality).
    ❌ Swallows the *reason* a resolution failed (an ``AmbiguousBinding``
       looks identical to "nothing bound" from the caller's point of view).
       Accepted — every call site already logged nothing more specific
       than "could not resolve" before this shim existed; this is a
       structural improvement (one function, not four), not a regression.

Thread safety:  N/A — a pure function, no shared state.
Async safety:   ✅ Synchronous — ``container.get()`` itself is synchronous.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from providify import DIContainer

__all__ = ["_resolve_optional"]


def _resolve_optional(container: DIContainer, cls: Any) -> Any | None:
    """
    Resolve ``cls`` from ``container``, or ``None`` on any failure.

    Args:
        container: The ``DIContainer`` to resolve from.
        cls: The class to request. Never ``None`` — callers guard the
            "was a class even given" branch themselves (``bind_mcp_adapter``/
            ``bind_skill_adapter`` accept ``client_cls: type | None = None``
            and only call this helper when it is not ``None``).

    Returns:
        ``container.get(cls)``'s result, or ``None`` if resolution raised
        for any reason (unbound interface, ambiguous binding, ...).

    Edge cases:
        - Swallows every exception from ``container.get()`` — this is a
          deliberate best-effort resolution, not a validated dependency;
          see the module docstring's DESIGN block for why a broad catch is
          the right shape here specifically.

    Example::

        client = _resolve_optional(container, MyClientClass) or MyClientClass(base_url=url)
    """
    try:
        return container.get(cls)
    except Exception:  # noqa: BLE001 - best-effort resolution, see module DESIGN block
        return None
