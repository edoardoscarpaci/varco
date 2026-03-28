"""
varco_core.service.mixin
========================
Marker base class for all ``AsyncService`` mixins.

All service-layer mixins in this package inherit from ``ServiceMixin`` so that
it is easy to identify them programmatically (``issubclass`` / ``isinstance``)
and visually in IDEs.

``ServiceMixin`` itself has **no abstract methods** — it is a pure marker.
Concrete behaviour is added by each mixin by overriding the composable hooks
defined in ``AsyncService`` (``_scoped_params``, ``_check_entity``,
``_prepare_for_create``, ``_after_create``, ``_after_update``,
``_after_delete``, ``_validate_entity``, ``_validate_entity_async``,
``_pre_check``).

DESIGN: non-generic ABC marker
    ``ServiceMixin`` is intentionally **not** generic.  All type parameters
    (``D``, ``PK``, ``C``, ``R``, ``U``) are already carried through the MRO
    by ``AsyncService[D, PK, C, R, U]``.  Duplicating them on the marker would
    require every mixin to repeat the same five TypeVar parameters without
    adding any type-safety benefit.  Keeping the marker non-generic also lets
    mixins that have no generic behaviour (e.g. ``AuditLogMixin``) inherit it
    cleanly.

DESIGN: ABC (not Protocol)
    Protocols describe *structural* subtyping — "if you have these methods you
    satisfy the contract".  Mixins require *nominal* subtyping — they must be
    explicitly listed in the MRO.  An ABC with no abstract methods is therefore
    the right tool: it provides ``issubclass`` / ``isinstance`` semantics
    without imposing any method contract.

    ✅ ``issubclass(CacheServiceMixin, ServiceMixin)``  → True
    ✅ ``isinstance(service, ServiceMixin)``             → True for any
       concrete service that inherits at least one mixin
    ✅ IDEs / linters see the marker in the MRO and can warn on misuse
    ❌ Does not validate that the mixin's ``super()`` chains are correct —
       that remains a developer responsibility enforced by tests.

Thread safety:  N/A — marker class, no state.
Async safety:   N/A — marker class, no async methods.
"""

from __future__ import annotations

from abc import ABC


class ServiceMixin(ABC):
    """
    Marker base class for all ``AsyncService`` mixins.

    Inherit from this class to signal that your class is designed to be
    composed into an ``AsyncService`` subclass via Python's MRO.  No methods
    need to be overridden — this class exists solely as a marker.

    Example::

        class MyScopingMixin(ServiceMixin, AsyncService[D, PK, C, R, U], Generic[D, PK, C, R, U]):
            def _scoped_params(self, params, ctx):
                # inject custom filter …
                return super()._scoped_params(params, ctx)
    """
