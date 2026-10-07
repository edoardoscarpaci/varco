"""
varco_fastapi.auth.trust_store
==============================

⚠️ DEPRECATED (Plan 026 / T3c, §D-T3-oq1) — removed in 5.0.0 (deprecated since 3.1.0; floor 2027-09-05).

``varco_fastapi.auth.trust_store.TrustStore`` is now a thin subclass of
``varco_core.tls.TrustStore`` that **pins its 3.0 semantics** (non-recursive folder scan,
``("*.pem", "*.crt")`` cert patterns, deferred mTLS-pairing validation) so every existing
construction of this type keeps producing a byte-identical ``ssl.SSLContext`` after upgrading
to 3.1 — zero behaviour change for any config expressible in 3.0.

**Migrate to** ``varco_core.tls.TrustStore`` — it is recursive by default, globs a wider
cert-file set (``varco_core.tls.CERT_FILE_PATTERNS``), validates mTLS pairing eagerly at
construction, and is reachable from any backend (not just ``varco_fastapi``). Pair it with
``varco_core.tls.ReloadingTrustStore`` for hot reload.

DESIGN: subclass over a plain re-export alias (§D-T3-oq1)
    ✅ A plain alias is not available here — unlike AB-1 (``render_rls_ddl``) and AB-2
       (``SchemaMigrationError``), the old and new names do **not** denote the same
       behaviour: the new type is recursive by default and globs ``*.cer`` too. Aliasing
       would silently give every existing ``varco_fastapi.auth.TrustStore`` user recursive,
       wider cert discovery on upgrade — precisely what the locked "Cert search" decision
       (``BACKLOG.md:30``) exists to prevent.
    ✅ ``include_system_cas`` semantics are preserved *by construction*: the field lives on
       the base class with the same default and the same meaning.
    ✅ The api-surface snapshot records a class's *defining module*
       (``design/api-freeze-and-standards/measurements/api-surface.md``). A subclass keeps
       that module unchanged — the ``varco_fastapi`` entry does not move at all; only a new
       ``varco_core`` row is added, a non-failing note. ``api_surface.py --check`` stays green.
    ❌ **The asymmetry**: ``isinstance(core_store, varco_fastapi.auth.TrustStore)`` is
       ``False``. A user who constructs the *new* type and passes it to their own function
       that ``isinstance``-checks the *old* one will get a surprise. Mitigation: no code in
       this repo does such a check on the legacy type
       (``varco_fastapi/tests/test_http_connection.py:110`` checks ``isinstance(ts,
       TrustStore)`` where ``TrustStore`` is this very legacy alias and ``ts`` comes from
       ``HttpConnectionSettings.to_trust_store()``, which by design still returns a legacy
       instance — see §D-T3-bridge — so that check keeps passing unchanged), and the
       ``DeprecationWarning`` tells the user to stop importing the old name.
    ❌ Two live classes instead of one until 5.0.0. Accepted — that is what a deprecation
       window is.

The ``DeprecationWarning`` fires **at construction**, not at import — merely having
``from varco_fastapi import TrustStore`` at a module's top does not warn.

Thread safety:  ✅ ``frozen=True`` — immutable after construction, same as the base class.
Async safety:   ✅ ``build_ssl_context()`` is synchronous, same as the base class.
"""

from __future__ import annotations

import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from varco_core.deprecation import warn_deprecated
from varco_core.tls.store import TrustStore as _CoreTrustStore
from varco_core.tls.store import normalize_ca_folders

if TYPE_CHECKING:
    from varco_core.connection.ssl import SSLConfig


@dataclass(frozen=True)
class TrustStore(_CoreTrustStore):
    """
    ⚠️ Deprecated — use ``varco_core.tls.TrustStore`` instead. Removed in 5.0.0.

    TLS trust configuration — merges system CAs, env-configured cert folders, explicit CA
    certs, and optional mTLS client identity. This subclass pins the exact behaviour this
    type had in 3.0: non-recursive cert-folder scanning, ``("*.pem", "*.crt")`` patterns, and
    a partial-mTLS check deferred to ``build_ssl_context()`` (not eager at construction, unlike
    the base ``varco_core.tls.TrustStore``).

    Attributes:
        ca_cert:            Explicit CA PEM bytes or path to PEM file (inherited).
        ca_folder:          Directory of ``*.pem`` / ``*.crt`` files to merge with the CA
                            trust chain — the 3.0 field name, singular. Internally folded
                            into the inherited ``ca_folders`` tuple in ``__post_init__`` so
                            ``build_ssl_context()`` (inherited from the base class, unchanged)
                            loads it exactly as before.
        client_cert:        Path to mTLS client certificate file (inherited).
        client_key:         Path to mTLS client private key file (inherited).
        include_system_cas: Whether to include the OS CA bundle (inherited).

    Thread safety:  ✅ frozen=True — safe to share across threads.
    Async safety:   ✅ ``build_ssl_context()`` is synchronous.

    Edge cases:
        - ``TrustStore()`` (default) → system CA bundle only.
        - ``include_system_cas=False`` with ``ca_cert=None`` and no ``ca_folder`` creates an
          empty CA store — all TLS connections will fail. Use only for strict pinning.
        - ``client_cert``/``client_key`` partial config does **not** raise at construction
          (unlike the base class) — it raises in ``build_ssl_context()``, exactly as in 3.0.

    Example::

        # Standard (system CAs only)
        ts = TrustStore()
        ctx = ts.build_ssl_context()

        # With a private CA
        ts = TrustStore(ca_cert=Path("/etc/ssl/my-ca.pem"))

        # From env vars
        ts = TrustStore.from_env()
    """

    ca_folder: Path | None = None

    # 3.0-semantics overrides — same field names/positions as the base class, narrower
    # defaults so an existing construction keeps producing a byte-identical context.
    cert_patterns: tuple[str, ...] = ("*.pem", "*.crt")
    recursive: bool = False

    def __post_init__(self) -> None:
        # Deliberately does NOT call super().__post_init__() — the base class's eager
        # __post_init__ both normalises ca_folders AND validates mTLS pairing; this subclass
        # must normalise (so build_ssl_context(), inherited unchanged, sees a proper tuple)
        # but must NOT validate eagerly (3.0 deferred that check to build_ssl_context() —
        # frozen behaviour, §D-T3-model).
        folders = normalize_ca_folders(self.ca_folders)
        if self.ca_folder is not None:
            folders = (*(folders or ()), Path(self.ca_folder))
        object.__setattr__(self, "ca_folders", folders)

        # Plan 058 / §D-floor: routed through warn_deprecated() so the floor test reads the
        # literal since=/removed_in= pair and 052's ledger records the fire. stacklevel=4
        # counts from inside the helper: warn_deprecated (1) → __post_init__ (2) → the
        # dataclass-generated __init__ (3) → the code that constructed the store (4) — the
        # same frame the pre-058 direct warnings.warn(stacklevel=3) blamed.
        warn_deprecated(
            "varco_fastapi.auth.TrustStore",
            since="3.1.0",
            removed_in="5.0.0",
            replacement=(
                "varco_core.tls.TrustStore (recursive, wider cert globs, any backend — see "
                "technical_docs/features/tls-trust-and-hot-reload.md)"
            ),
            stacklevel=4,
        )

    # ── Factory methods ────────────────────────────────────────────────────────

    @classmethod
    def from_env(cls) -> TrustStore:
        """
        Build a ``TrustStore`` from standard environment variables — 3.0 behaviour, unchanged.

        **Plan 048 / DI-6**: delegates to ``varco_core.tls.settings.TlsSettings`` for the
        actual env read — the four names moved from ``VARCO_CA_CERT``/``VARCO_CLIENT_CERT``/
        ``VARCO_CLIENT_KEY``/``VARCO_TRUST_STORE_DIR`` to ``VARCO_TLS_*`` (§D-prefix rule 2).

        Returns:
            A fully populated ``TrustStore``.

        Edge cases:
            - Missing env vars produce ``None`` values — the resulting ``TrustStore`` uses
              system CAs only.
            - Paths are not validated at construction time — ``build_ssl_context()`` will
              raise if a path does not exist.
        """
        from varco_core.tls.settings import TlsSettings

        settings = TlsSettings()
        return cls(
            ca_cert=settings.ca_cert,
            ca_folder=settings.trust_store_dir,
            client_cert=settings.client_cert,
            client_key=settings.client_key,
        )

    def to_ssl_config(self) -> SSLConfig:
        """
        Convert this ``TrustStore`` to a ``varco_core.connection.SSLConfig`` — 3.0 behaviour,
        unchanged.

        Prefer ``varco_core.tls.TrustStore.to_ssl_config()`` (the base class's own, lossless
        for ``verify``/``check_hostname``) for new code — see the module docstring's
        migration note.

        Returns:
            ``SSLConfig`` with equivalent CA/client cert configuration.

        Edge cases:
            - ``ca_cert`` as ``bytes`` cannot be expressed as a ``Path`` in ``SSLConfig`` — the
              returned config has ``ca_cert=None`` in that case.
            - ``include_system_cas=False`` is not representable in ``SSLConfig`` — lost.
            - ``verify=True`` and ``check_hostname=True`` are always set on the returned
              ``SSLConfig`` — this legacy bridge always verifies, exactly as in 3.0
              (§D-T3-bridge: the *lossy* bridge this plan intentionally leaves in place for
              ``HttpConnectionSettings.to_trust_store()``'s return type).
        """
        from varco_core.connection.ssl import SSLConfig  # noqa: PLC0415

        ca_cert_path: Path | None = self.ca_cert if isinstance(self.ca_cert, Path) else None

        return SSLConfig(
            ca_cert=ca_cert_path,
            ca_folder=self.ca_folder,
            client_cert=self.client_cert,
            client_key=self.client_key,
            verify=True,
            check_hostname=True,
        )

    @classmethod
    def system(cls) -> TrustStore:
        """
        Return a ``TrustStore`` using only the OS CA bundle. Equivalent to ``TrustStore()``.

        Returns:
            A ``TrustStore`` with ``include_system_cas=True`` and no extras.
        """
        return cls()

    # ── SSL context builder ────────────────────────────────────────────────────

    def build_ssl_context(self) -> ssl.SSLContext:
        """
        Build and return an ``ssl.SSLContext`` — byte-identical to 3.0's output for any
        config expressible in 3.0.

        The mTLS pairing check that the base class runs eagerly at construction is deferred
        here to match 3.0's behaviour exactly.

        Raises:
            ValueError: Exactly one of ``client_cert``/``client_key`` is set.
            FileNotFoundError: A configured path does not exist.
            ssl.SSLError: A certificate fails to load.

        Thread safety:  ✅ Creates a new context per call.
        Async safety:   ✅ Synchronous — call at startup, not per-request.
        """
        if (self.client_cert is None) != (self.client_key is None):
            raise ValueError(
                "TrustStore: 'client_cert' and 'client_key' must both be set or "
                "both be None for mTLS.  Got: "
                f"client_cert={self.client_cert!r}, client_key={self.client_key!r}"
            )
        # ca_folders/cert_patterns/recursive/include_system_cas/ca_cert are all inherited,
        # unchanged fields — the base class's build_ssl_context() already implements the
        # exact 3.0 ordering for them (§D-T3-model: the two predecessor orderings already
        # agreed on everything they share), so no override of the context-assembly logic
        # itself is needed — only the deferred validation above.
        return _CoreTrustStore.build_ssl_context(self)


__all__ = ["TrustStore"]
