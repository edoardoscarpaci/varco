"""
varco_core.auth.di
=====================
``enable_authorization_audit()`` — DI wiring for `AuditingAuthorizer`
(Plan 036 / S11, Step 25, §D-S11-shape).

**§D-036-oq2 (this plan's Open Question 2), resolved**: this lives in
`varco_core`, not `varco_fastapi`. `AuditingAuthorizer` has no HTTP
dependency (it wraps `AbstractAuthorizer`, a `varco_core` interface, and
injects `AbstractEventProducer`, also `varco_core`) — the same reasoning
that puts `varco_casbin.di.enable_policy_authorizer` in the package that
*owns the implementation* it is wiring, not in the package that happens to
serve HTTP.

Mirrors `varco_core.revocation.di.enable_token_revocation`'s `enable_*`
shape: an opt-in DI binding that would shadow an app default if
auto-registered, never a scanned `@Configuration` (`container.scan(
"varco_core", recursive=True)` is a documented, in-use pattern that would
otherwise silently wrap every scanning app's authorizer).

⚠️ **Order-sensitive — call it last.** `enable_authorization_audit()`
resolves the *currently bound* `AbstractAuthorizer` and re-binds the
wrapper. Calling it before the application registers its own authorizer
wraps `BaseAuthorizer` and is then shadowed by the app's later, real
binding — the wrapper never runs. There is no way to detect "a
higher-priority binding is coming later" from inside the container (no
such concept exists at call time), so this is a documented convention, not
a runtime-enforced one; `enable_authorization_audit()` still fails loudly
via `RuntimeError` on the ONE case it *can* detect algorithmically — being
called twice on an already-wrapped delegate.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from providify import Provider

from varco_core.auth.audit import AuditDecisionPolicy, AuditingAuthorizer
from varco_core.auth.base import AbstractAuthorizer
from varco_core.event.producer import AbstractEventProducer

if TYPE_CHECKING:
    from providify import DIContainer

__all__ = ["enable_authorization_audit"]


def enable_authorization_audit(
    container: DIContainer,
    *,
    policy: AuditDecisionPolicy = AuditDecisionPolicy.DENIALS,
) -> DIContainer:
    """
    Opt in to authorization-decision auditing: resolve whatever
    `AbstractAuthorizer` is currently bound, wrap it in an
    `AuditingAuthorizer`, and re-bind the wrapper as the application's
    `AbstractAuthorizer`.

    Args:
        container: The `DIContainer`, already scanned/wired with the
            application's real `AbstractAuthorizer` binding and an
            `AbstractEventProducer` binding — call this **last**, after
            both exist (see the module docstring's "call it last" note).
        policy: `AuditDecisionPolicy` — default `DENIALS`.

    Returns:
        The same container, for chaining.

    Raises:
        RuntimeError: The currently-bound `AbstractAuthorizer` is already
            an `AuditingAuthorizer` — calling this twice would double
            wrap (and double record) every decision. A no-op would hide
            the mistake; this fails loudly instead (Edge cases table).

    Example::

        container = DIContainer()
        container.scan("myapp", recursive=True)   # binds the real authorizer
        enable_authorization_audit(container)      # call LAST
    """
    delegate: AbstractAuthorizer = container.get(AbstractAuthorizer)
    if isinstance(delegate, AuditingAuthorizer):
        raise RuntimeError(
            "enable_authorization_audit() was already applied to this container's "
            "AbstractAuthorizer binding — calling it a second time would wrap the "
            "wrapper and double-record every decision."
        )
    producer: AbstractEventProducer = container.get(AbstractEventProducer)
    wrapped = AuditingAuthorizer(delegate, producer, policy=policy)

    def _provide_auditing_authorizer() -> AbstractAuthorizer:
        return wrapped

    container.provide(
        Provider(singleton=True)(_provide_auditing_authorizer),
        returns=AbstractAuthorizer,
    )
    return container
