"""
app.py
======
Application factory for the ``08-field-encryption`` example.

Demonstrates field-level at-rest encryption with ``varco_sa``:

- ``Patient.ssn`` and ``Patient.notes`` are annotated with ``EncryptedHint()``
- ``SAModelFactory.build(Patient, encryptor=enc)`` generates the ORM model so that
  the mapper transparently encrypts on INSERT/UPDATE and decrypts on SELECT.
- The service, router, and DTO layers always operate on plaintext strings —
  encryption is completely transparent above the repository layer.

Key hierarchy used in this example
-----------------------------------
``FernetFieldEncryptor`` — single-key AES-128-CBC + HMAC-SHA256 (via ``cryptography``).

Bootstrap sequence
------------------
1. The encryptor is created (or provided by the caller).
2. ``SQLAlchemyRepositoryProvider.from_components()`` is built directly —
   bypassing the DI-auto-register path — so ``factory.build(Patient, encryptor=enc)``
   is called explicitly with the encryptor.
3. The provider is exposed to the DI container via a ``@Provider`` binding.
4. ``sa_bootstrap(container)`` scans ``varco_sa`` and discovers ``SAConfiguration`` —
   but no ``entity_classes`` are passed, so no automatic re-build of the Patient
   mapper without the encryptor occurs.
5. ``create_tables(container)`` runs on startup — creates the ``patients`` table.
6. FastAPI starts accepting requests.

DESIGN: manual RepositoryProvider construction over DI-auto-register
    The SA DI stack calls ``provider.register(*entity_classes)`` which calls
    ``factory.build(cls)`` WITHOUT passing the encryptor.  To inject an
    encryptor we must pre-build the mapper explicitly and expose the pre-wired
    provider to the container.

    ✅ Clean — the encryptor is injected at exactly the right level (mapper).
    ✅ The rest of the DI stack is unchanged — PatientService and PatientAssembler
       are still resolved via standard DI.
    ❌ Bypasses some DI-auto-wiring — the caller must build and expose the
       provider explicitly.  For production multi-entity setups, consider adding
       an ``encryptor_map`` to ``SAConfig`` or using a ``@PostConstruct`` hook.

Thread safety:  ✅ ``create_app()`` is called once at module import.
Async safety:   ✅ No async operations at factory time — ``create_tables``
                   runs inside the lifespan startup hook.
"""

from __future__ import annotations

import os

# Import @Singleton-decorated classes so their DI metadata is stamped
# before the container tries to resolve them.
from assembler import PatientAssembler  # noqa: F401 — registers PatientAssembler
from fastapi import FastAPI
from keys import generate_ephemeral_encryptor
from models import Patient
from providify import DIContainer, Provider
from router import add_health_route, make_patient_router
from service import PatientService  # noqa: F401 — registers PatientService
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from varco_core.encryption import FieldEncryptor
from varco_core.service.base import IUoWProvider
from varco_fastapi.di import bootstrap as fastapi_bootstrap
from varco_sa.config import SAConfig
from varco_sa.di import bootstrap as sa_bootstrap
from varco_sa.di import create_tables
from varco_sa.provider import SQLAlchemyRepositoryProvider

from varco_fastapi import create_varco_app

# ── Shared SQLAlchemy DeclarativeBase ──────────────────────────────────────────


# DESIGN: module-level Base (one per process)
#   ✅ All SA ORM classes generated from entity_classes land in the same
#      metadata — ``create_all`` creates all tables in one call.
#   ❌ Running ``create_app()`` twice in the same process would attempt to
#      re-register the same table in the same metadata — SAModelFactory caches
#      by entity class, but both calls share the same Base.
#      Tests call ``create_app()`` once per session to avoid this.
class Base(DeclarativeBase):
    """Shared SQLAlchemy declarative base for this example."""


# ── DI container bootstrap ─────────────────────────────────────────────────────


def _build_container(db_url: str, encryptor: FieldEncryptor) -> DIContainer:
    """
    Build and configure a ``DIContainer`` for the patient records service.

    Key steps:
    1. Build a ``SQLAlchemyRepositoryProvider`` directly (bypassing DI's
       auto-register) so ``SAModelFactory.build(Patient, encryptor=enc)`` is
       called with the encryptor — enabling transparent encryption at the mapper.
    2. Expose the pre-wired provider to the container via ``@Provider``.
    3. Register ``SAConfig`` for framework tooling (health checks, ``create_tables``).
    4. Run ``varco_fastapi.di.bootstrap()`` for framework defaults.
    5. Scan ``varco_sa`` (via ``sa_bootstrap``) to discover ``SAConfiguration`` — but
       pass NO ``entity_classes`` so DI does not auto-rebuild the Patient mapper
       without the encryptor.

    Args:
        db_url:    PostgreSQL connection URL with ``postgresql+asyncpg://`` scheme.
        encryptor: ``FieldEncryptor`` to inject into the SA mapper so that
                   ``ssn`` and ``notes`` are encrypted transparently.

    Returns:
        A fully configured ``DIContainer``.
    """
    container = DIContainer()

    engine = create_async_engine(db_url, echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # ── 1. Build the mapper and repository provider with the encryptor ────────
    # DESIGN: inject the encrypted mapper directly into the provider's _built dict.
    #
    #   The DI-auto-register path (provider.register()) calls
    #   provider._factory.build(cls) WITHOUT the encryptor.  We bypass that
    #   by calling provider._factory.build() ourselves WITH the encryptor and
    #   storing the result directly in provider._built — the same dict that
    #   get_repository() and make_uow() read from.
    #
    #   This uses provider._built (private) but avoids any other workaround:
    #   ✅ No monkey-patching, no subclassing, no DI hacks.
    #   ✅ The provider's factory is used — its Base is already set correctly.
    #   ❌ provider._built is technically private — fragile if the name changes.
    #      Acceptable for an example; production apps should add a public API.
    provider = SQLAlchemyRepositoryProvider.from_components(
        base=Base,
        session_factory=session_factory,
    )
    # Build the ORM model with the encryptor on the provider's own factory.
    # provider._factory is the SAModelFactory(base=Base) created by from_components().
    # Calling build() here with the encryptor populates the factory's cache AND
    # the Base.metadata with the patients table (with LargeBinary columns for ssn/notes).
    built_pair = provider._factory.build(Patient, encryptor=encryptor)  # type: ignore[attr-defined]
    # Store into _built so get_repository() and make_uow() find Patient registered.
    provider._built[Patient] = built_pair  # type: ignore[attr-defined]

    # ── 2. Register the pre-wired provider as IUoWProvider ───────────────────
    # SAConfiguration.uow_provider (discovered by scan) re-exposes RepositoryProvider
    # as IUoWProvider.  By binding IUoWProvider directly with our pre-built
    # provider we bypass that chain and ensure the encrypted mapper is used.
    #
    # DESIGN: bind IUoWProvider directly
    #   ✅ Simplest path — AsyncService.__init__ injects IUoWProvider directly.
    #   ✅ Avoids any resolution of the scan-discovered SQLAlchemyRepositoryProvider
    #      (which would call factory.build(Patient) WITHOUT the encryptor).
    #   ❌ Bypasses SAConfiguration.uow_provider — acceptable since it only re-exports
    #      the same singleton and does no additional work.
    @Provider(singleton=True)
    def _uow_provider() -> IUoWProvider:
        """Returns the pre-wired provider (with encrypted Patient mapper) as IUoWProvider."""
        return provider  # type: ignore[return-value]

    container.provide(_uow_provider)

    # ── 3. Register SAConfig for framework tooling (create_tables, health) ────
    @Provider(singleton=True)
    def _sa_config() -> SAConfig:
        return SAConfig(
            engine=engine,
            base=Base,
            # DESIGN: empty entity_classes — prevents DI auto-registration of
            # Patient without the encryptor.  The mapper is pre-built above.
            entity_classes=(),
        )

    container.provide(_sa_config)

    # ── 4. Bootstrap varco_fastapi ─────────────────────────────────────────────
    # 4.0 (Plan 049): the framework @Configuration is VarcoFastAPIConfiguration; the
    # 4.0 hand-wired form is the package bootstrap() (scan-installs it, idempotent,
    # so create_varco_app's FastAPIModule re-running it later is safe).
    fastapi_bootstrap(container)

    # ── 4b. Permissive authorizer ─────────────────────────────────────────────
    from varco_core.auth.authorizer import BaseAuthorizer  # noqa: PLC0415
    from varco_core.auth.base import AbstractAuthorizer  # noqa: PLC0415

    # 4.0 (Plan 053 / FLIP-5): a class-keyed bind of BaseAuthorizer is gated
    # exactly like the scanned fallback — run with VARCO_SECURITY_ENV=development
    # (see README). A production app registers a real AbstractAuthorizer.
    container.bind(AbstractAuthorizer, BaseAuthorizer)

    # ── 5. Scan varco_sa (no entity_classes) ─────────────────────────────────
    # Discovers SAConfiguration and SQLAlchemyRepositoryProvider scan targets, but
    # entity_classes=() means no automatic Patient mapper build without encryptor.
    sa_bootstrap(container)

    # ── 6. Register example-local @Singleton classes ──────────────────────────
    # container.scan() requires an installed package string; local example modules
    # are not installed packages, so we bind them explicitly.
    container.bind(PatientAssembler, PatientAssembler)
    container.bind(PatientService, PatientService)

    return container


# ── Application factory ────────────────────────────────────────────────────────


def create_app(
    db_url: str | None = None,
    *,
    encryptor: FieldEncryptor | None = None,
) -> FastAPI:
    """
    Build and return the configured FastAPI application.

    This is the canonical entry point used by both uvicorn (``app:app``) and
    the test suite (``create_app(postgres_url, encryptor=enc)``).

    Args:
        db_url:    PostgreSQL connection URL.  When ``None``, reads from the
                   ``DATABASE_URL`` environment variable.
        encryptor: ``FieldEncryptor`` to use for ``ssn`` and ``notes``.
                   When ``None``, an ephemeral key is generated (suitable for
                   tests and demos; not for production).

    Returns:
        A ``FastAPI`` instance ready to serve requests.

    Raises:
        KeyError: ``DATABASE_URL`` is not set and ``db_url`` is ``None``.

    Edge cases:
        - Calling ``create_app()`` twice in the same process may cause SA
          ``InvalidRequestError`` because ``Base.metadata`` is module-level.
          Tests call ``create_app()`` once per session to avoid this.
        - The ephemeral encryptor generates a new random key on every call to
          ``create_app()``.  Data encrypted in one app instance cannot be
          decrypted by another.  For production, pass a stable encryptor.
    """
    url = db_url or os.environ["DATABASE_URL"]

    # Use caller-supplied encryptor or generate a fresh ephemeral one.
    # DESIGN: default to ephemeral so tests and quick demos work out of the box.
    #   ✅ Zero configuration for demos — just call create_app(url).
    #   ❌ Data is irrecoverable after process restart — document clearly.
    active_encryptor = encryptor or generate_ephemeral_encryptor()

    container = _build_container(url, active_encryptor)
    PatientRouter = make_patient_router(container)

    app = create_varco_app(
        container,
        routers=[PatientRouter],
        title="Field Encryption Example",
        version="0.1.0",
        description=(
            "Demonstrates varco field-level at-rest encryption:\n\n"
            "- ``ssn`` and ``notes`` are stored as ciphertext in PostgreSQL.\n"
            "- The REST API always receives and returns **plaintext** — encryption is transparent.\n"
            "- ``FernetFieldEncryptor`` provides AES-128-CBC + HMAC-SHA256.\n\n"
            "**Note**: this demo uses an ephemeral key — data is not recoverable after restart."
        ),
        # validate=False because PatientRouter has no _auth ClassVar — intentional
        # for this auth-free example.
        validate=False,
    )

    # Register a startup event to create tables after the lifespan boots.
    @app.on_event("startup")
    async def _create_schema() -> None:
        """Create all SA-managed tables on startup (idempotent DDL)."""
        await create_tables(container)

    # Add a simple liveness probe.
    add_health_route(app)

    return app


# ── Module-level app for ``uvicorn app:app`` ─────────────────────────────────
# Only built when DATABASE_URL is present — avoids KeyError in test contexts.
app: FastAPI | None = None

try:
    if "DATABASE_URL" in os.environ:
        app = create_app()
except Exception:
    pass


__all__ = ["app", "create_app", "Base"]
