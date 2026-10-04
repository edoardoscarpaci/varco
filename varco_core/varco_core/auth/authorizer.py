"""
varco_core.auth.authorizer
==========================
Development-only permissive ``AbstractAuthorizer`` — allows every operation
unconditionally, and is a DI binding **only** under
``VARCO_SECURITY_ENV=development`` (Plan 053 / FLIP-5).

``BaseAuthorizer``
    A no-op implementation, declared ``@Fallback`` and
    ``@Requires(env="VARCO_SECURITY_ENV", value="development")``. In a
    development process it is a candidate only when no application binding
    of ``AbstractAuthorizer`` is active. In any other process (variable
    unset, ``production``, ``dev``, ``Development``, ``""``) it is **not a
    binding at all**: ``container.get(AbstractAuthorizer)`` raises
    providify's ``LookupError("No binding found for 'AbstractAuthorizer'.
    Did you forget container.bind() or container.provide()?")``,
    ``container.validate()`` reports ``MISSING_BINDING`` at every service
    that injects it (plus one ``CONDITION_INACTIVE`` naming the variable),
    and ``SecurityPosture`` reports ``posture.authorizer_unbound`` (HIGH).

DESIGN: development-only binding (Plan 053 / FLIP-5)
    ✅ Fail-closed (research brief 010 §3): a deployment that forgot to
       register a real authorizer refuses to start (or to construct its
       first service) instead of silently allowing everything.
    ✅ One variable shared with ``SecurityPostureSettings.environment``,
       compared against the same exact literal ``"development"`` — the
       binding and the posture demotion can never disagree.
    ✅ Lazy: providify re-reads ``os.environ`` on every resolution, so a
       test's ``monkeypatch.setenv`` is honoured on the next ``get()``; no
       varco predicate code, no container state.
    ❌ A bare-container caller sees providify's generic ``LookupError`` text
       ("Did you forget container.bind()") — read ``validate()``'s
       ``CONDITION_INACTIVE`` issue or ``varco diagnose`` for the reason.
    ❌ A code-configured ``SecurityPostureSettings(environment="development")``
       does NOT open the gate: the marker is a zero-arg predicate evaluated
       during resolution with no container in reach, and resolving settings
       from the container there would recurse. Export the variable instead.
    Alternatives rejected: escalating a posture finding only (does not
    refuse to bind under ``enforce="warn"``); raising in ``__init__``
    (``validate()`` would be clean for a binding guaranteed to fail); a
    second ``VARCO_AUTHORIZER_ALLOW_PERMISSIVE`` variable (two knobs for one
    axis — a security weakening an operator could flip without review).

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

from providify import Fallback, Requires, Singleton

from varco_core.auth.base import AbstractAuthorizer, Action, AuthContext, Resource


@Fallback
@Requires(env="VARCO_SECURITY_ENV", value="development")
@Singleton
class BaseAuthorizer(AbstractAuthorizer):
    """
    Permissive fallback authorizer — allows every operation unconditionally.

    How to get this binding:
        ``VARCO_SECURITY_ENV=development`` (exact, case-sensitive — the only
        switch). In any other environment this class is not a binding;
        register your own ``AbstractAuthorizer``. Under ``development`` it is
        ``@Fallback``, so any other ``AbstractAuthorizer`` binding wins
        regardless of registration order.

    Running permissive in production deliberately:
        Bind an *instance* — ``bind_instance(container, AbstractAuthorizer,
        BaseAuthorizer())`` or a ``@Provider`` returning ``BaseAuthorizer()``.
        An instance/provider binding carries no class markers, so it resolves
        in every environment; ``posture.base_authorizer_bound`` (WARN)
        reports it. ``container.bind(AbstractAuthorizer, BaseAuthorizer)``
        does NOT work — a class-keyed binding reads ``@Requires`` off this
        class and is gated exactly like the scanned one.

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
        - ``VARCO_SECURITY_ENV`` flipped in-process after this singleton was
          resolved under ``development``: the gate is re-checked on every
          lookup (providify 2.1.1), so the next ``get()`` raises
          ``LookupError`` (fails closed); the cached instance is **not**
          evicted, and flipping back returns the same object
          (``test_flip_hides_cached_singleton_without_evicting``). A test
          that needs a clean slate uses a fresh container.
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
        # Only reachable in development or via an explicit instance binding
        # (Plan 053 / FLIP-5) — production apps register a real authorizer.
