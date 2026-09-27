"""
Regression tests — Plan 011 / RD-3, drift item 4.

User reports: ``request.state.varco_request_context`` (set by
``LocalizationMiddleware``) was never read by ``varco_fastapi/exceptions.py``
or ``ErrorMiddleware`` — so ``message_resolver=`` was never passed to
``error_message_for()``, meaning an error response's ``message`` field was
never actually catalog-localized, despite i18n being enabled and a locale
being resolved. Correct behaviour: with a ``MessageCatalog`` wired (threaded
through ``create_varco_app(i18n=...)``), a ``ServiceNotFoundError`` raised
from a route handler renders its ``message`` in the resolved locale.
"""

from __future__ import annotations

from httpx import ASGITransport, AsyncClient
from providify import DIContainer, Provider
from varco_core.exception.service import ServiceNotFoundError
from varco_core.i18n.catalog import DictMessageCatalog
from varco_core.i18n.settings import I18nSettings
from varco_fastapi.app import create_varco_app
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.presets import GenericRouter


class _SomeEntity:
    pass


class _BoomRouter(GenericRouter):
    _prefix = "/test"

    @route("GET", "/boom")
    async def boom(self):
        raise ServiceNotFoundError(entity_id="1", entity_cls=_SomeEntity)


@Provider(singleton=True)
def _fr_catalog() -> DictMessageCatalog:
    return DictMessageCatalog({"fr": {"varco.error.not_found": "Introuvable"}})


async def test_regression_error_message_is_catalog_localized_via_route_handler() -> None:
    # This exercises the FastAPI @app.exception_handler path
    # (add_exception_handlers) — a ServiceException raised from inside a
    # route handler is caught by Starlette's ExceptionMiddleware, which
    # sits INSIDE LocalizationMiddleware, so the ContextVar is technically
    # still live here — but add_exception_handlers() never read the
    # catalog/locale AT ALL before this fix, so the message was always the
    # untranslated English default regardless.
    from varco_fastapi.modules.edge import ErrorEnvelopeModule, LocalizationModule

    container = DIContainer()
    container.provide(_fr_catalog)
    app = create_varco_app(
        container,
        routers=[_BoomRouter],
        modules=[
            LocalizationModule(i18n=I18nSettings(enabled=True, supported_locales=("en", "fr"))),
            ErrorEnvelopeModule(),
        ],
        validate=False,
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/test/boom?lang=fr")

    assert response.headers.get("Content-Language") == "fr"
    assert response.json()["message"] == "Introuvable"


async def test_regression_error_message_is_english_default_without_catalog() -> None:
    # No catalog wired -> byte-identical to pre-fix behaviour: the
    # untranslated default_message, no silent breakage.
    from varco_fastapi.modules.edge import ErrorEnvelopeModule, LocalizationModule

    container = DIContainer()
    app = create_varco_app(
        container,
        routers=[_BoomRouter],
        modules=[
            LocalizationModule(i18n=I18nSettings(enabled=True, supported_locales=("en", "fr"))),
            ErrorEnvelopeModule(),
        ],
        validate=False,
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/test/boom?lang=fr")

    body = response.json()
    assert body["message"] != "Introuvable"


async def test_regression_error_middleware_reads_request_state_mirror_directly() -> None:
    # RD-3's documented ordering hazard, exercised directly against
    # ErrorMiddleware: by the time ErrorMiddleware runs (it sits OUTSIDE
    # LocalizationMiddleware), the ambient ContextVar this middleware set
    # has already been reset by LocalizationMiddleware's own `finally` —
    # request.state.varco_request_context (the RD-3 mirror) is the only
    # place the resolved locale is still reachable. Exercised directly
    # against ErrorMiddleware rather than through the full app stack
    # (extra_middleware= ends up OUTSIDE ErrorMiddleware too, per
    # create_varco_app's documented ordering, so it cannot reproduce this
    # ordering hazard end-to-end without reaching into internal wiring).

    from starlette.requests import Request
    from varco_core.context.request import RequestContext
    from varco_fastapi.middleware.error import ErrorMiddleware

    catalog = DictMessageCatalog({"fr": {"varco.error.not_found": "Introuvable"}})
    middleware = ErrorMiddleware.__new__(ErrorMiddleware)
    middleware._include_trace_id = False
    middleware._message_catalog = catalog
    middleware._set_content_language = True

    request = Request(scope={"type": "http", "headers": []})
    request.scope["state"] = {}
    request.state.varco_request_context = RequestContext(locale="fr")

    exc = ServiceNotFoundError(entity_id="1", entity_cls=_SomeEntity)
    response = middleware._service_error_response(exc, request)

    assert response.headers.get("content-language") == "fr"
    import json

    body = json.loads(bytes(response.body))
    assert body["message"] == "Introuvable"
