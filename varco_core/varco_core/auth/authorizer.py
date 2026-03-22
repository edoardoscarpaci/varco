"""
varco_core.base_authorizer
==============================
Default permissive ``AbstractAuthorizer`` — allows every operation
unconditionally so simple applications work with zero authorization setup.

``BaseAuthorizer``
    A no-op implementation registered at priority ``-(2**31)`` (the lowest
    sensible priority).  All calls to ``authorize()`` return immediately
    without inspecting the context, action, or resource.

    Authorization is **opt-in**: register any ``AbstractAuthorizer``
    subclass at the default priority (0) or higher and it will
    automatically shadow this binding.

``_FALLBACK_PRIORITY``
    Named constant for the priority value used by ``BaseAuthorizer``.
    Equivalent to a 32-bit signed integer minimum — signals "absolute
    last resort" and leaves the full range of negative integers (``-1``
    through ``-2_147_483_647``) free for intermediate-priority fallbacks.

    Why not just ``-1``?
    Using ``-(2**31)`` ensures ``BaseAuthorizer`` is never accidentally
    shadowed by a custom fallback at ``-1``, and no existing application
    code needs to be re-prioritized when intermediate layers are added.

DESIGN: permissive by default, not deny-by-default
    ✅ Simple CRUD applications work out of the box — no grants, no
       ``AuthContext`` configuration, no authorizer boilerplate.
    ✅ Incrementally adoptable — start with no auth, add a custom
       authorizer only when the application actually needs it.
    ✅ Framework is usable for internal tools, prototypes, and scripts
       where auth overhead is not justified.
    ❌ A developer who forgets to register a real authorizer ships a
       permissive app silently.  Mitigate with a startup assertion or
       environment check in production::

           assert not isinstance(container.get(AbstractAuthorizer), BaseAuthorizer), \
               "No real authorizer registered — refusing to start in production"

DESIGN: @Singleton(priority=_FALLBACK_PRIORITY) without a qualifier
    Adding a qualifier (e.g. ``qualifier="base"``) would opt this class
    *out* of the default ``Inject[AbstractAuthorizer]`` resolution, making
    it invisible as a fallback.  No qualifier is intentional — the
    priority mechanism alone is sufficient and correct.

Thread safety:  ✅ Stateless — no instance state.
Async safety:   ✅ ``authorize`` is a no-op coroutine.
"""

from __future__ import annotations

from typing import Final

from providify import Singleton

from varco_core.auth.base import AbstractAuthorizer, Action, AuthContext, Resource

# Lowest sensible priority — equivalent to a 32-bit signed integer minimum.
# Leaves the full range -1 … -2_147_483_647 free for intermediate-priority
# fallback authorizers without any re-prioritization of existing bindings.
_FALLBACK_PRIORITY: Final[int] = -(2**31)


@Singleton(priority=_FALLBACK_PRIORITY)
class BaseAuthorizer(AbstractAuthorizer):
    """
    Permissive fallback authorizer — allows every operation unconditionally.

    Used automatically by the DI container when no application-specific
    ``AbstractAuthorizer`` is registered (it has the absolute lowest
    priority: ``-(2**31)``).

    When to replace:
        Register a custom ``@Singleton`` at any priority ≥ ``-(2**31)+1``
        (in practice, the default ``0`` is fine).  The container will
        resolve your implementation instead — no changes to service code
        or DI wiring required.

    When NOT to replace:
        - Prototypes and internal tools with no access-control requirements.
        - Integration test suites focused on business logic, not permissions.
        - Early development before authorization rules are defined.

    Thread safety:  ✅ Stateless — no instance state.
    Async safety:   ✅ No-op coroutine — returns immediately.

    Edge cases:
        - ``ctx``, ``action``, and ``resource`` are accepted but never
          inspected — all callers are unconditionally allowed.
        - An anonymous ``AuthContext`` (``user_id=None``) with no grants is
          treated identically to an admin context — this implementation
          makes no distinction.
    """

    async def authorize(
        self,
        ctx: AuthContext,
        action: Action,
        resource: Resource,
    ) -> None:
        """
        Allow unconditionally — no checks performed.

        Args:
            ctx:      Ignored.
            action:   Ignored.
            resource: Ignored.

        Returns:
            ``None`` always — every operation is permitted.
        """
        # Intentionally a no-op — all operations are allowed.
        # Replace this binding with a real authorizer for production use.
