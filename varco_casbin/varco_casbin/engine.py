"""
varco_casbin.engine
===================
``CasbinPolicyEngine`` — the concrete policy backend.

Implements both halves of the ``varco_core.auth.policy`` seam by wrapping a
``casbin.AsyncEnforcer``:

    PolicyEngine.enforce(request)   → enforcer.enforce(sub, [dom,] obj, act)
    PolicyManagement.*              → enforcer.add/remove/get_named_policy + role API

One class, two interfaces — bound to both in DI so high-traffic service code
resolves it as ``PolicyEngine`` and the REST router as ``PolicyManagement``.

ABAC bridging (``_AttrStr``)
---------------------------
Casbin ABAC matchers read attributes off the request object
(``r.obj.owner_id``, ``"admin" in r.sub.roles``).  But RBAC/ACL matchers and
the role manager (``g(r.sub, p.sub)``) need ``r.sub`` / ``r.obj`` to behave as
plain *strings*.  ``_AttrStr`` is a ``str`` subclass that also carries the
attribute bag — so the SAME wrapped value satisfies both:

    - ``r.sub == p.sub`` / ``g(r.sub, p.sub)``  → string identity ✅
    - ``r.obj.owner_id``                        → attribute access ✅

This lets one engine serve ACL, RBAC, RBAC-with-domains, and ABAC models
without per-model branching in ``enforce``.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import casbin
from providify import Fallback, Inject, PostConstruct, PreDestroy, Singleton
from varco_core.auth.policy import EnforcementRequest, PolicyEngine, PolicyManagement
from varco_core.di import BACKEND_PRIORITY

from varco_casbin.adapter import build_adapter
from varco_casbin.config import CasbinSettings

if TYPE_CHECKING:
    from collections.abc import Mapping


class _AttrStr(str):
    """
    A ``str`` that also exposes a mapping's entries as attributes.

    Used to pass Casbin a request subject/object that behaves as a plain
    string for ACL/RBAC matchers *and* carries ABAC attributes for
    attribute matchers — see the module docstring.

    Thread safety:  ✅ Effectively immutable after construction.
    Async safety:   ✅ Pure value.

    Edge cases:
        - An attribute name that collides with a ``str`` method (e.g. a field
          literally named ``"strip"``) would shadow that method on this
          instance.  Domain field names rarely collide; rename the field or
          add it to ``RequestMapper.object_attr_excludes`` if it does.
        - Missing attributes raise ``AttributeError`` exactly like a normal
          object — an ABAC matcher referencing an absent field fails closed.
        - ``__reduce__`` makes this class safe under ``copy.deepcopy``
          (Casbin's ``load_policy()`` deepcopies ``self.model`` internally,
          and once an ``_AttrStr`` has been threaded into the model/role-
          manager state via a prior ``enforce()`` call, an uninformed
          deepcopy would try to reconstruct it via ``cls(value)`` — missing
          the required ``attrs`` argument).  A field named literally
          ``"_attrs"`` would collide with the internal stash used for this
          and be overwritten; as with the method-name collision above, this
          is expected to be vanishingly rare for real domain field names.
    """

    # Declared (not assigned) so mypy recognizes the object.__setattr__ below
    # as populating a known instance attribute rather than an arbitrary one.
    _attrs: dict[str, Any]

    # str is immutable, so attribute values must be attached in __new__ where
    # the instance already exists; __init__ would also work but __new__ keeps
    # construction in one place.
    def __new__(cls, value: str, attrs: Mapping[str, Any]) -> _AttrStr:
        obj = super().__new__(cls, value)
        for key, val in attrs.items():
            # object.__setattr__ bypasses any __setattr__ override and works
            # on the (otherwise immutable) str subclass instance dict.
            object.__setattr__(obj, key, val)
        # Stash the original attrs mapping too (not just the individual
        # attributes copied onto __dict__) so __reduce__ can reconstruct an
        # equivalent instance without having to reverse-engineer __dict__.
        object.__setattr__(obj, "_attrs", dict(attrs))
        return obj

    def __reduce__(self) -> tuple[type[_AttrStr], tuple[str, dict[str, Any]]]:
        # WHY: copy.deepcopy's default reconstruction for a str subclass
        # calls `cls.__new__(cls, value)` — passing only the wrapped string,
        # never the extra `attrs` kwarg this class's __new__ requires. Casbin
        # calls copy.deepcopy(self.model) inside load_policy(), and once an
        # _AttrStr has been threaded into the model/role-manager state via a
        # prior enforce() call, an uninformed deepcopy raises `TypeError:
        # _AttrStr.__new__() missing 1 required positional argument: 'attrs'`.
        # __reduce__ tells copy/pickle to call `_AttrStr(str(self), attrs)`
        # instead, restoring both the string value and the attribute bag.
        return (self.__class__, (str(self), self._attrs))


@Fallback
@Singleton(priority=BACKEND_PRIORITY, qualifier="casbin")
class CasbinPolicyEngine(PolicyEngine, PolicyManagement):
    """
    Casbin-backed ``PolicyEngine`` + ``PolicyManagement``.

    Holds a single long-lived ``casbin.AsyncEnforcer`` built from the model
    (``CasbinSettings.resolve_model_text``) and the adapter
    (``build_adapter``).  Must be a **shared singleton** — a per-call engine
    would rebuild the enforcer and reload policy on every request.

    Lifecycle:
        ``start()`` (``@PostConstruct``) builds the enforcer and loads policy
        from the durable store.  ``stop()`` (``@PreDestroy``) is a no-op hook
        retained for symmetry / future connection cleanup.  The class is also
        an async context manager for non-DI usage.

    Thread safety:  ❌ Not thread-safe — the enforcer holds mutable policy.
                    Use from a single event loop.
    Async safety:   ⚠️ ``enforce`` is lock-free (read path); all *mutations*
                    are serialized by a lazily-created ``asyncio.Lock`` so
                    concurrent writes cannot corrupt enforcer state.

    Edge cases:
        - ``enforce`` before ``start()`` raises ``RuntimeError`` — fail closed.
        - With ``adapter="memory"`` there is no durable store; ``reload`` is a
          no-op and policy is lost on restart.

    Example::

        engine = CasbinPolicyEngine(CasbinSettings(model_preset="rbac"))
        async with engine:
            await engine.add_role_for_user("alice", "admin")
            await engine.add_policy("admin", "*", "*")
            assert await engine.enforce(
                EnforcementRequest(subject="alice", object="posts", action="read")
            )
    """

    def __init__(self, settings: Inject[CasbinSettings]) -> None:
        """
        Args:
            settings: Casbin configuration injected from the container.
        """
        self._settings = settings
        # Enforcer is built in start() — None until then so a premature
        # enforce() can fail closed with a clear error.
        # casbin ships no type stubs — AsyncEnforcer resolves to Any.
        self._enforcer: casbin.AsyncEnforcer | None = None  # type: ignore[no-any-unimported]
        self._adapter: Any | None = None
        # Lazy lock — never create asyncio primitives at __init__ time; the
        # event loop may not exist yet (CLAUDE.md async-safety rule).
        self._lock: asyncio.Lock | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def _get_lock(self) -> asyncio.Lock:
        """Return the write lock, creating it inside the running loop on first use."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    @PostConstruct
    async def start(self) -> None:
        """
        Build the enforcer and load policy from the durable store.

        Idempotent guard: a second call while already started raises
        ``RuntimeError`` rather than silently rebuilding (which would drop
        in-memory edits).

        Raises:
            RuntimeError: Already started.
            ValueError:   Invalid model preset / missing adapter parameters
                          (propagated from settings / ``build_adapter``).
        """
        if self._enforcer is not None:
            raise RuntimeError("CasbinPolicyEngine.start() called twice.")

        # Build the model from text (preset / file / inline).
        model = casbin.Model()
        model.load_model_from_text(self._settings.resolve_model_text())

        # Build the adapter (None for memory mode).
        self._adapter = build_adapter(self._settings)

        # The async SQLAlchemy adapter needs its `casbin_rule` table to exist
        # before the first read.  create_table() is idempotent (CREATE TABLE IF
        # NOT EXISTS), so it is safe to call on every start.  Guarded by hasattr
        # so the file/memory adapters (which lack it) are skipped.
        create_table = getattr(self._adapter, "create_table", None)
        if callable(create_table):
            await create_table()

        # AsyncEnforcer cannot await in __init__, so we construct then load.
        enforcer = casbin.AsyncEnforcer(model, self._adapter)
        enforcer.enable_auto_save(self._settings.auto_save)

        # Only load when there is a durable store; a None adapter has nothing
        # to load and AsyncEnforcer.load_policy would have no adapter to await.
        if self._adapter is not None:
            await enforcer.load_policy()

        self._enforcer = enforcer

    @PreDestroy
    async def stop(self) -> None:
        """
        Release engine resources.

        Currently a no-op (the SQLAlchemy adapter manages its own engine
        lifecycle); retained as the symmetric teardown hook and async
        context-manager exit point.
        """
        self._enforcer = None
        self._adapter = None

    async def __aenter__(self) -> CasbinPolicyEngine:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    # casbin ships no type stubs — AsyncEnforcer resolves to Any.
    def _require_enforcer(self) -> casbin.AsyncEnforcer:  # type: ignore[no-any-unimported]
        """Return the started enforcer or raise — fail closed before start()."""
        if self._enforcer is None:
            raise RuntimeError(
                "CasbinPolicyEngine used before start(). Call start() / enter the "
                "async context, or bootstrap via DI so @PostConstruct runs."
            )
        return self._enforcer

    # ── PolicyEngine (hot path) ───────────────────────────────────────────────

    async def enforce(self, request: EnforcementRequest) -> bool:
        """
        Evaluate ``request`` against the active Casbin model + policy.

        Wraps ``subject`` / ``object`` in ``_AttrStr`` so the same value works
        for string matchers (ACL/RBAC) and attribute matchers (ABAC).  When
        ``request.domain`` is set, the domain is passed as the second column
        (``r = sub, dom, obj, act``) for the RBAC-with-domains model.

        Args:
            request: The engine-neutral authorization question.

        Returns:
            ``True`` if the policy permits the request, ``False`` otherwise.

        Raises:
            RuntimeError: The engine has not been started.

        Async safety: ✅ Lock-free read; safe to call concurrently with other
                      ``enforce`` calls.  A mutation racing an ``enforce`` is
                      eventually consistent (last write wins).
        """
        enforcer = self._require_enforcer()
        sub = _AttrStr(request.subject, request.subject_attrs)
        obj = _AttrStr(request.object, request.object_attrs)

        # enforce() is synchronous (CPU-bound matcher eval, no I/O).
        if request.domain is not None:
            return bool(enforcer.enforce(sub, request.domain, obj, request.action))
        return bool(enforcer.enforce(sub, obj, request.action))

    # ── PolicyManagement (cold, admin) ────────────────────────────────────────

    async def add_policy(self, *values: str, ptype: str = "p") -> bool:
        """Add a ``ptype`` policy rule; ``False`` if it already exists."""
        async with self._get_lock():
            return bool(await self._require_enforcer().add_named_policy(ptype, *values))

    async def remove_policy(self, *values: str, ptype: str = "p") -> bool:
        """Remove a ``ptype`` policy rule; ``False`` if no match existed."""
        async with self._get_lock():
            return bool(await self._require_enforcer().remove_named_policy(ptype, *values))

    async def list_policies(self, ptype: str = "p") -> list[tuple[str, ...]]:
        """List ``ptype`` policy rules as token tuples."""
        # get_named_policy is a synchronous in-memory read.
        rows = self._require_enforcer().get_named_policy(ptype)
        return [tuple(row) for row in rows]

    async def add_role_for_user(
        self,
        user: str,
        role: str,
        domain: str | None = None,
    ) -> bool:
        """Grant ``role`` to ``user`` (optionally scoped to ``domain``)."""
        async with self._get_lock():
            enforcer = self._require_enforcer()
            if domain is not None:
                return bool(await enforcer.add_role_for_user_in_domain(user, role, domain))
            return bool(await enforcer.add_role_for_user(user, role))

    async def remove_role_for_user(
        self,
        user: str,
        role: str,
        domain: str | None = None,
    ) -> bool:
        """Revoke ``role`` from ``user`` (optionally scoped to ``domain``)."""
        async with self._get_lock():
            enforcer = self._require_enforcer()
            if domain is not None:
                # No delete_role_for_user_in_domain in this casbin version —
                # remove the underlying grouping rule (user, role, domain).
                return bool(await enforcer.remove_grouping_policy(user, role, domain))
            return bool(await enforcer.delete_role_for_user(user, role))

    async def roles_for_user(
        self,
        user: str,
        domain: str | None = None,
    ) -> list[str]:
        """List the roles assigned to ``user`` (optionally within ``domain``)."""
        enforcer = self._require_enforcer()
        if domain is not None:
            return list(await enforcer.get_roles_for_user_in_domain(user, domain))
        return list(await enforcer.get_roles_for_user(user))

    async def reload(self) -> None:
        """
        Reload policy from the durable store, discarding unsaved in-memory edits.

        No-op for the in-memory adapter (nothing to reload from).
        """
        if self._adapter is None:
            return
        async with self._get_lock():
            await self._require_enforcer().load_policy()

    def __repr__(self) -> str:
        started = self._enforcer is not None
        return (
            f"CasbinPolicyEngine(preset={self._settings.model_preset!r}, "
            f"adapter={self._settings.adapter!r}, started={started})"
        )


__all__ = [
    "CasbinPolicyEngine",
]
