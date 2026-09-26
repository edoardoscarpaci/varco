"""
varco_core.di.errors
======================
Exception types for Plan 047's config-driven backend selection and
catalogue (§D-check, §D-introspect). Kept in their own bare module (no
providify import) so any caller can catch them without paying for the
rest of ``varco_core.di``.
"""

from __future__ import annotations


class BackendSelectionError(RuntimeError):
    """
    Raised when a ``VARCO_<SEAM>_BACKEND`` value cannot be resolved to a
    live binding.

    Covers three distinct failure shapes, always naming the offending
    environment variable and value in the message:

    - **Unknown named value** — the value is not the seam's default name
      and not among the values ``selected_by()`` recorded in this process
      (a typo, wrong case, or a backend package that was never
      bootstrapped). Raised by ``check_backend_selection()``.
    - **Unresolvable dotted path** — ``pkg.mod:Name`` where the module
      cannot be imported, the attribute does not exist, or (for a class
      target) the class does not subclass the seam's interface. Raised by
      ``bind_env_overrides()`` at bootstrap time, never at first ``get()``.
    - **Wrong instance type at first resolution** — a callable dotted
      target that returns an object failing ``isinstance`` against the
      seam's interface. Raised lazily, wrapping the underlying error.

    DESIGN: one exception type for all three shapes
        ✅ A caller wrapping ``bootstrap()``/``create_varco_app()`` needs
           only one ``except`` clause to catch every "backend selection is
           broken" case.
        ❌ The message, not the type, is what distinguishes the three
           shapes — acceptable, since every message names the env var and
           value explicitly (Edge cases in the plan's own §Edge cases
           table all rely on substring assertions, not exception
           subtypes).
    """


class CatalogueError(RuntimeError):
    """
    Raised by ``varco_core.di.introspect.catalogue()`` when a fallback
    binding cannot be classified into any bucket (seam default/backend,
    feature default/twin, settings, or "other default"), or when
    ``Catalogue.merge_backend_entries()`` finds two packages registering
    the same ``(seam, value)`` pair.

    DESIGN: raise, never silently omit
        A fallback binding that fits no bucket is either a new varco
        default nobody taught the catalogue about, or a plugin's backend
        colliding with a shipped value — both are catalogue bugs, not
        catalogue gaps to paper over with an "unknown" row. Raising turns
        the omission into a build failure (``scripts/di_catalogue.py``,
        ``make lint``) instead of a silently stale generated document.
    """


__all__ = ["BackendSelectionError", "CatalogueError"]
