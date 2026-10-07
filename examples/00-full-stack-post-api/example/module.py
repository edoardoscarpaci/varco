"""
example.module
================
``PostAppModule`` — the example app's own ``VarcoModule`` (Plan 049 / DI-1,
§D-example). This is the "write your own module" reference the guide
points to: an app that composes ``varco_core``/``varco_fastapi`` behind
``create_varco_app(container, modules=[*default_modules(), ..., PostAppModule(...)])``
instead of hand-building ``FastAPI`` + ``VarcoLifespan``.

What this module owns
----------------------
- ``_register()``: scans the ``example`` package (discovering
  ``PostAssembler``/``PostService``/``PostEventConsumer``/``StreamsRouter``),
  self-binds ``PostRouter``/``PostService`` (both have abstract bases the
  scanner does not self-bind), and binds the already-constructed
  ``AbstractServerAuth`` the app built before the container existed.
  ``RedisEventBusSettings(use_streams=True)`` is **not** bound here — a
  settings override must exist before any module registers, so
  ``create_app()`` binds it ahead of ``create_varco_app()``.
  ``bind_repositories(container, Post)`` is **not** called here — the app's
  own ``SAModule(entities=(Post,))`` (``app.py``) already does it.
- ``async_init()``: ``await create_tables(container)`` (DDL — not for
  production migrations, same caveat the pre-049 docstring carried),
  ``await registry.load_all()`` (issuer keyset), then mounts the three
  app-specific routers (``PostRouter``, ``AuthRouter``, ``StreamsRouter``) —
  all three need DI-injected constructor arguments
  (``PostService``/``WebSocketEventBus``/``SSEEventBus``) or a hand-built
  value (``authority``/``server_auth``) that ``create_varco_app``'s
  ``_mount_router`` (a synchronous, no-argument ``router_cls().build_router()``
  call) cannot supply — this is exactly the ``_mount_router`` limitation
  §D-example's Step 19 flagged and resolved: mount from here, not from
  ``routers=``.
- ``start()``/``stop()``: register/stop ``PostEventConsumer`` to the bus —
  the bus itself is started/stopped by ``CoreModule`` (§D-order).

Thread safety:  ⚠️ Single construction per app process, same as any module.
Async safety:   ✅ ``async_init``/``start``/``stop`` perform I/O.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from varco_core.di.module import VarcoModule
from varco_redis.modules import RedisModule
from varco_sa.modules import SAModule
from varco_ws.modules import WsModule

if TYPE_CHECKING:
    from providify import DIContainer
    from varco_core.authority.jwt_authority import JwtAuthority
    from varco_core.authority.registry import TrustedIssuerRegistry
    from varco_fastapi.auth.server_auth import AbstractServerAuth


class PostAppModule(VarcoModule):
    """
    The Post domain's own ``VarcoModule`` — depends on the four backend
    modules whose bootstrap it relies on (``SAModule`` for the ORM,
    ``RedisModule`` for the event bus + cache, ``WsModule`` for the two
    push adapters, ``CoreModule`` transitively through all three).

    Args:
        authority: The already-built ``JwtAuthority`` — passed to
            ``AuthRouter`` for token signing.
        registry: The already-built ``TrustedIssuerRegistry`` — its
            ``load_all()`` is awaited in ``async_init()``.
        server_auth: The already-built ``AbstractServerAuth`` — bound into
            the container (so ``FastAPIModule`` installs
            ``RequestContextMiddleware``) and passed to ``PostRouter``.
    """

    name = "post_app"
    depends_on = (SAModule, RedisModule, WsModule)

    def __init__(
        self,
        authority: JwtAuthority,
        registry: TrustedIssuerRegistry,
        server_auth: AbstractServerAuth,
    ) -> None:
        super().__init__()
        self._authority = authority
        self._registry = registry
        self._server_auth = server_auth
        self._app: Any | None = None

    def _register(self, container: DIContainer) -> None:
        from varco_core.di import bind_instance  # noqa: PLC0415
        from varco_fastapi.auth.server_auth import AbstractServerAuth  # noqa: PLC0415

        from example.router import PostRouter  # noqa: PLC0415
        from example.service import PostService  # noqa: PLC0415

        # Makes FastAPIModule's middleware() install RequestContextMiddleware
        # (§D-http's FastAPIModule row: "iff AbstractServerAuth is resolvable").
        bind_instance(container, AbstractServerAuth, self._server_auth)

        container.scan("example", recursive=True)
        # PostRouter/PostService both extend abstract bases the scanner does
        # not self-bind (VarcoCRUDRouter/AsyncService+CacheServiceMixin) —
        # same root cause, same explicit self-bind fix pre-049 carried.
        container.bind(PostRouter, PostRouter)
        container.bind(PostService, PostService)

        # NOTE: bind_repositories(container, Post) is NOT called here —
        # SAModule(entities=(Post,)) (app.py) already does it; a second call
        # would raise AlreadyBoundError (bind_repositories is a non-fallback,
        # call-once-per-entity registration, same as any other bind_*).

    def middleware(self, container: DIContainer) -> list[Any]:
        return []

    def configure_app(self, app: Any, container: DIContainer) -> None:
        self._app = app

    async def async_init(self, container: DIContainer) -> None:
        from varco_sa.di import create_tables  # noqa: PLC0415

        from example.auth import AuthRouter  # noqa: PLC0415
        from example.router import PostRouter  # noqa: PLC0415
        from example.service import PostService  # noqa: PLC0415
        from example.streams import StreamsRouter  # noqa: PLC0415

        # Not for production migrations — use Alembic (MigrationsModule).
        await create_tables(container)
        await self._registry.load_all()

        # §D-example: PostRouter/AuthRouter/StreamsRouter all need either a
        # DI-injected constructor argument or a hand-built value
        # `_mount_router` (a synchronous, no-argument `router_cls()` call)
        # cannot supply — mounted here instead, after the async DI
        # resolution `_mount_router` cannot perform either.
        post_service: PostService = await container.aget(PostService)
        post_router = PostRouter(service=post_service, auth=self._server_auth).build_router()
        auth_router = AuthRouter(authority=self._authority).build_router()
        streams_inst: StreamsRouter = await container.aget(StreamsRouter)
        streams_router = streams_inst.build_router()

        assert self._app is not None  # noqa: S101 — configure_app() always runs first
        self._app.include_router(post_router)
        self._app.include_router(auth_router)
        self._app.include_router(streams_router)

    # Plan 051 / DI-5, §D-consumer: PostEventConsumer is a @Singleton
    # EventConsumer bound in the container — CoreModule.start() discovers
    # and registers it to the app's AbstractEventBus automatically, once the
    # bus has started, and stops it (reverse order) before the bus stops.
    # No start()/stop() override is needed here any more.


__all__ = ["PostAppModule"]
