"""
varco_core.webhook.module
============================
``WebhookModule`` — the explicit, DI-wired home for ``WebhookDispatcher``
(Plan 050 / §D-webhook, DI-10 row 4-5).

Before this module, ``WebhookDispatcher`` was always hand-constructed
(``WebhookDispatcher(repository=repo); dispatcher.register_to(bus,
dlq=dlq)``) — there was no ``@Fallback`` dispatcher provider because a
``@Requires``-gated provider cannot express "iff a repository is bound"
(``providify/decorator/scope.py:818-823`` — see the module's own §D-webhook
rejected alternative). A plain ``VarcoModule`` sidesteps that: it is never
scanned (049 §D-scan), so it can register a fallback in-memory repository
itself, then a dispatcher provider over whatever
``WebhookSubscriptionRepository`` ends up bound — the app's own
(``VARCO_WEBHOOK_BACKEND=sa``/``beanie``), a hand-supplied one
(``WebhookModule(repository=...)``), or the in-memory default.

DESIGN — a ``VarcoModule``, never a ``@Fallback`` dispatcher provider in
``varco_core.di.bootstrap()`` (§D-webhook)
    ✅ An app that wants webhooks lists exactly one more module — no
       hidden default in every bare container.
    ✅ ``acollect(container, EventConsumer)`` (051) finds the dispatcher
       provider via providify's subclass-matching ``_filter()``
       (``container.py:2051-2057``) with no ``@Multibound`` re-declaration.
    ❌ Rejected alternative — a ``@Fallback`` dispatcher provider registered
       by ``varco_core.di.bootstrap()``: requires a repository binding to
       exist in every default container (or a
       ``NullWebhookSubscriptionRepository``, making "webhooks silently do
       nothing" the default — the opposite of what a webhook operator
       wants).

Thread safety:  ⚠️ Single-threaded startup only (same contract as every
                    ``VarcoModule``).
Async safety:   ✅ ``components()``'s ``start``/``stop`` are ``async def``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

# Module-level imports (NOT inside _register/components) — required so the
# @Provider closures declared in _register() below can resolve their
# Inject[...] parameter annotations against *this module's* __globals__.
# Under PEP 563 (`from __future__ import annotations`), an annotation is a
# lazy string providify evaluates later against the defining function's
# own module globals — a name imported only inside the function body is
# invisible to that lookup (the same "quoted @Provider return annotations"
# pitfall CLAUDE.md documents, here hitting parameters instead of a return
# type). See varco_ws/di.py's identical fix for adapter factories.
from providify import Fallback, Inject, InjectMeta, Provider

from varco_core.di import BACKEND_PRIORITY, bind_instance, is_user_bound
from varco_core.di.lifecycle import Phase
from varco_core.di.module import CoreModule, VarcoModule
from varco_core.event.dlq import AbstractDeadLetterQueue
from varco_core.settings.bind import bind_settings
from varco_core.webhook.base import (
    InMemoryWebhookSubscriptionRepository,
    WebhookSubscriptionRepository,
)
from varco_core.webhook.dispatcher import WebhookDispatcher
from varco_core.webhook.settings import WebhookSettings

if TYPE_CHECKING:
    from collections.abc import Sequence

    from providify import DIContainer

    from varco_core.di.lifecycle import LifecycleComponent

__all__ = ["WebhookModule"]


class WebhookModule(VarcoModule):
    """
    DI-wires ``WebhookDispatcher`` against a ``WebhookSubscriptionRepository``
    (given, or the in-memory ``@Fallback`` default) and the bound
    ``WebhookSettings``/optional ``AbstractDeadLetterQueue`` (§D-wire row 4).

    Args:
        repository: An already-constructed repository to bind
            (``bind_instance``), e.g. a durable ``SAWebhookSubscriptionRepository``
            built by hand. ``None`` (default) registers the ``@Fallback``
            ``InMemoryWebhookSubscriptionRepository`` instead — a durable
            backend selected via ``VARCO_WEBHOOK_BACKEND=sa``/``beanie``
            still wins over this fallback (both are ``@Fallback``, but the
            backend's own priority tier outranks the bare in-memory one —
            045's two-tier model).
        settings: An already-constructed ``WebhookSettings`` to bind. `None``
            (default) leaves ``CoreModule``'s ``bind_core_settings()``
            binding in place (every ``WebhookModule`` still ``depends_on``
            ``CoreModule``, which registers it first).
        enabled: The usual Plan 047 tri-state.

    Example::

        app = create_varco_app(container, modules=[CoreModule(), WebhookModule()])
    """

    name = "webhook"
    phase = Phase.CONSUMERS
    depends_on = (CoreModule,)

    def __init__(
        self,
        repository: WebhookSubscriptionRepository | None = None,
        *,
        settings: WebhookSettings | None = None,
        enabled: bool | None = None,
    ) -> None:
        super().__init__(enabled=enabled)
        self._given_repository = repository
        self._given_settings = settings

    def _register(self, container: DIContainer) -> None:
        if self._given_settings is not None:
            bind_settings(container, WebhookSettings, instance=self._given_settings)

        if self._given_repository is not None:
            if not is_user_bound(container, WebhookSubscriptionRepository):
                bind_instance(container, WebhookSubscriptionRepository, self._given_repository)
        elif not container.is_resolvable(WebhookSubscriptionRepository):
            # No app-selected durable backend (VARCO_WEBHOOK_BACKEND=sa/
            # beanie) is bound either — register the bare in-memory
            # @Fallback default, at the SAME priority tier every other
            # bare-default seam uses (045's two-tier model: BACKEND_PRIORITY
            # still outranks this if a backend module registers later).
            @Fallback
            @Provider(singleton=True)
            def _in_memory_webhook_repository() -> InMemoryWebhookSubscriptionRepository:
                return InMemoryWebhookSubscriptionRepository()

            # returns= override: a closure-local factory's PEP-563 return
            # annotation cannot resolve against this method's own globals
            # (CLAUDE.md's "quoted @Provider return annotations" pitfall) —
            # providify ≥ 2.0.0's returns= kwarg sidesteps it (Plan 016 / RL-2).
            container.provide(_in_memory_webhook_repository, returns=WebhookSubscriptionRepository)

        # Declared injections — `dlq` is optional (InjectMeta(optional=True))
        # so an app with no bound AbstractDeadLetterQueue still resolves
        # this provider (§D-wire row 4: "declared on the provider method,
        # never container.get() inside").
        def _dispatcher_provider(
            repository: Inject[WebhookSubscriptionRepository],
            settings: Inject[WebhookSettings],
            dlq: Annotated[AbstractDeadLetterQueue | None, InjectMeta(optional=True)] = None,
        ) -> WebhookDispatcher:
            return WebhookDispatcher(repository=repository, settings=settings, dlq=dlq)

        container.provide(
            Provider(singleton=True, priority=BACKEND_PRIORITY)(_dispatcher_provider),
            returns=WebhookDispatcher,
        )

    def components(self, container: DIContainer) -> Sequence[LifecycleComponent]:
        """
        Two components (§D-components): ``webhook.repository`` (``INFRA`` —
        the repository's own ``start()``/``stop()``, a no-op for the
        in-memory default) and ``webhook.dispatcher`` (``CONSUMERS`` — a
        no-op start/stop; the dispatcher is auto-registered to the bus by
        ``CoreModule``'s ``core.consumers`` component via 051's
        ``acollect``, present here only so the module is visible in the
        realised plan).
        """
        from varco_core.di.lifecycle import LifecycleComponent  # noqa: PLC0415

        async def _start_repository() -> None:
            if not container.is_resolvable(WebhookSubscriptionRepository):
                return
            repo: WebhookSubscriptionRepository = await container.aget(
                WebhookSubscriptionRepository
            )
            start = getattr(repo, "start", None)
            if callable(start):
                await start()

        async def _stop_repository() -> None:
            if not container.is_resolvable(WebhookSubscriptionRepository):
                return
            repo: WebhookSubscriptionRepository = await container.aget(
                WebhookSubscriptionRepository
            )
            stop = getattr(repo, "stop", None)
            if callable(stop):
                await stop()

        async def _noop() -> None:
            return None

        return [
            LifecycleComponent(
                name="webhook.repository",
                phase=Phase.INFRA,
                start=_start_repository,
                stop=_stop_repository,
            ),
            LifecycleComponent(
                name="webhook.dispatcher",
                phase=Phase.CONSUMERS,
                start=_noop,
                stop=_noop,
            ),
        ]
