"""
varco_core.base_authorizer
==============================
Default permissive ``AbstractAuthorizer`` — allows every operation
unconditionally so simple applications work with zero authorization setup.

``BaseAuthorizer``
    A no-op implementation, declared ``@Fallback`` — a candidate only when
    no application binding of ``AbstractAuthorizer`` is active. All calls
    to ``authorize()`` return immediately without inspecting the context,
    action, or resource.

    Authorization is **opt-in**: register any ``AbstractAuthorizer``
    binding — a ``@Singleton``, `container.bind()`, or `container.provide()`
    — in any order, and it wins over this fallback unconditionally
    (`technical_docs/features/dependency-injection.md`).

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

DESIGN: @Fallback @Singleton without a qualifier
    Adding a qualifier (e.g. ``qualifier="base"``) would opt this class
    *out* of the default ``Inject[AbstractAuthorizer]`` resolution, making
    it invisible as a fallback.  No qualifier is intentional — this remains
    true under ``@Fallback`` (`providify/container.py`'s qualifier
    narrowing runs before the fallback-drop, same as every other varco
    qualified default).

Thread safety:  ✅ Stateless — no instance state.
Async safety:   ✅ ``authorize`` is a no-op coroutine.
"""

from __future__ import annotations

from providify import Fallback, Singleton

from varco_core.auth.base import AbstractAuthorizer, Action, AuthContext, Resource


@Fallback
@Singleton
class BaseAuthorizer(AbstractAuthorizer):
    """
    Permissive fallback authorizer — allows every operation unconditionally.

    Used automatically by the DI container when no application-specific
    ``AbstractAuthorizer`` is registered — declared ``@Fallback``, so any
    other ``AbstractAuthorizer`` binding wins regardless of registration
    order.

    When to replace:
        Register any ``AbstractAuthorizer`` binding.  The container will
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
