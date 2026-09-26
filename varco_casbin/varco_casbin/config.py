"""
varco_casbin.config
===================
Configuration for the Casbin policy engine backend.

``CasbinSettings`` is the single injectable configuration object for
``CasbinPolicyEngine``.  It selects the Casbin **model** (the ``.conf`` that
defines whether you are doing ACL / RBAC / ABAC) and the **adapter** (where
policy rules are persisted).

Environment variables (prefix ``VARCO_CASBIN_``)
------------------------------------------------
::

    VARCO_CASBIN_MODEL_PRESET=rbac          # acl | rbac | rbac_domains | abac
    VARCO_CASBIN_MODEL_PATH=/etc/app.conf   # explicit model file (overrides preset)
    VARCO_CASBIN_ADAPTER=sqlalchemy         # memory | file | sqlalchemy | beanie
    VARCO_CASBIN_DB_URL=postgresql+asyncpg://localhost/app
    VARCO_CASBIN_DB_NAME=myapp              # required for adapter=beanie
    VARCO_CASBIN_POLICY_PATH=/etc/policy.csv
    VARCO_CASBIN_ADMIN_ROLE=admin
"""

from __future__ import annotations

from importlib import resources
from typing import Final, Literal

from pydantic import model_validator
from pydantic_settings import SettingsConfigDict
from varco_core.config import VarcoSettings

# Bundled model presets shipped as package data (varco_casbin/models/*.conf).
# Keyed by the VARCO_CASBIN_MODEL_PRESET value.
_PRESETS: Final[frozenset[str]] = frozenset({"acl", "rbac", "rbac_domains", "abac"})


# DESIGN: CasbinSettings is NOT decorated with @Singleton.
#   Providify resolves a @Singleton class by injecting its __init__ params, but
#   pydantic's BaseSettings.__init__ is ``(**values)`` — providify tries to
#   inject the ``values`` VAR_KEYWORD and raises LookupError.  Instead,
#   ``varco_casbin.di.bootstrap`` registers a provider that builds this via
#   ``from_env()`` (a clean zero-arg call), which is the deterministic,
#   override-friendly way to bind pydantic settings in providify.
class CasbinSettings(VarcoSettings):
    """
    Immutable configuration for ``CasbinPolicyEngine``.

    All fields are read from environment variables with the ``VARCO_CASBIN_``
    prefix and have defaults so a development engine (in-memory RBAC) works
    with no configuration at all.

    Model resolution order (first non-empty wins):
        1. ``model_text``   — inline model definition (tests, dynamic config).
        2. ``model_path``   — path to a ``.conf`` model file on disk.
        3. ``model_preset`` — one of the bundled presets (default ``"rbac"``).

    Adapter selection (``adapter`` field):
        - ``"memory"``     — non-persistent, policies live only in RAM (dev/tests).
        - ``"file"``       — CSV file at ``policy_path`` (single-process).
        - ``"sqlalchemy"`` — durable DB store at ``db_url`` (requires the
          ``varco-casbin[sqlalchemy]`` extra).  Needed for the dynamic
          persisted CRUD admin API in multi-process deployments.
        - ``"beanie"``     — durable MongoDB store (requires the
          ``varco-casbin[beanie]`` extra).  Needs both ``db_url`` and
          ``db_name``.  Ideal for Beanie-backed apps that want to avoid
          pulling in the SQLAlchemy stack for policy storage alone.

    Attributes:
        model_text:   Inline Casbin model text, or ``None``.
        model_path:   Filesystem path to a ``.conf`` model, or ``None``.
        model_preset: Bundled preset name — ``acl`` / ``rbac`` /
                      ``rbac_domains`` / ``abac``.
        adapter:      Persistence adapter — ``memory`` / ``file`` /
                      ``sqlalchemy`` / ``beanie``.
        policy_path:  CSV policy path for the ``file`` adapter.
        db_url:       Connection URL.  For ``sqlalchemy``: an async SQLAlchemy
                      URL.  For ``beanie``: a MongoDB connection string (e.g.
                      ``"mongodb://localhost:27017"``).
        db_name:      MongoDB database name — required for ``adapter="beanie"``.
                      Unused by other adapters.
        auto_save:    When ``True``, each mutation is persisted immediately
                      through the adapter (the basis of dynamic persisted CRUD).
        admin_role:   Role the management router requires by default.

    Thread safety:  ✅ frozen=True — immutable after construction.
    Async safety:   ✅ No mutable state.

    Edge cases:
        - ``adapter="sqlalchemy"`` with ``db_url=None`` is a configuration
          error — ``build_adapter`` raises ``ValueError`` rather than silently
          falling back to memory.
        - ``adapter="beanie"`` with ``db_url=None`` or ``db_name=None`` is a
          configuration error — both fields are required for the Beanie adapter.
        - An unknown ``model_preset`` raises ``ValueError`` at resolution time
          listing the valid presets.

    Example::

        # In-memory RBAC (default) — no env vars needed
        CasbinSettings()

        # Durable ABAC backed by Postgres
        CasbinSettings(model_preset="abac", adapter="sqlalchemy",
                       db_url="postgresql+asyncpg://localhost/app")

        # Durable RBAC backed by MongoDB (Beanie app — no SQLAlchemy needed)
        CasbinSettings(model_preset="rbac", adapter="beanie",
                       db_url="mongodb://localhost:27017", db_name="myapp")
    """

    # protected_namespaces=() — allow ``model_*`` field names (model_text /
    # model_path / model_preset).  Pydantic v2 reserves the ``model_`` prefix by
    # default, which suppresses the generated __init__ signature and breaks
    # providify constructor injection (it would see only ``**values``).
    model_config = SettingsConfigDict(
        env_prefix="VARCO_CASBIN_", frozen=True, protected_namespaces=()
    )

    model_text: str | None = None
    model_path: str | None = None
    model_preset: str = "rbac"

    adapter: Literal["memory", "file", "sqlalchemy", "beanie"] = "memory"
    policy_path: str | None = None
    db_url: str | None = None
    db_name: str | None = None

    auto_save: bool = True
    admin_role: str = "admin"

    @model_validator(mode="after")
    def _fail_fast_on_unknown_preset(self) -> CasbinSettings:
        """
        Plan 048 / DI-6: fail at ``bind_settings()`` time — not only at the
        first ``resolve_model_text()`` call — for a preset name that is
        obviously wrong, **without** narrowing ``model_preset`` to a
        ``Literal`` (which would forbid overriding it via ``model_text``/
        ``model_path``, the class's own documented resolution order:
        ``model_preset`` is irrelevant whenever either of those is set, so
        an invalid preset name paired with an explicit ``model_text`` must
        stay legal). Mirrors ``resolve_model_text()``'s own check exactly.
        """
        if not self.model_text and not self.model_path and self.model_preset not in _PRESETS:
            raise ValueError(
                f"Unknown Casbin model preset {self.model_preset!r}. "
                f"Valid presets: {sorted(_PRESETS)}. "
                f"Set VARCO_CASBIN_MODEL_PRESET to one of these, or supply "
                f"VARCO_CASBIN_MODEL_PATH / model_text for a custom model."
            )
        return self

    def resolve_model_text(self) -> str:
        """
        Return the effective Casbin model definition text.

        Applies the resolution order documented on the class: ``model_text``
        wins, then ``model_path`` (read from disk), then the bundled preset.

        Returns:
            The model ``.conf`` content as a string, ready for
            ``casbin.Model.load_model_from_text``.

        Raises:
            ValueError:        ``model_preset`` is not a known preset.
            FileNotFoundError: ``model_path`` is set but the file is missing.

        Edge cases:
            - An empty ``model_text`` (``""``) is treated as "not set" and
              falls through to ``model_path`` / preset.
        """
        # 1. Inline text — highest precedence (tests, dynamic config).
        if self.model_text:
            return self.model_text

        # 2. Explicit model file on disk.
        if self.model_path:
            # Let a missing path raise FileNotFoundError — fail fast and loud
            # rather than silently using a preset the operator did not choose.
            with open(self.model_path, encoding="utf-8") as fh:
                return fh.read()

        # 3. Bundled preset shipped as package data.
        if self.model_preset not in _PRESETS:
            raise ValueError(
                f"Unknown Casbin model preset {self.model_preset!r}. "
                f"Valid presets: {sorted(_PRESETS)}. "
                f"Set VARCO_CASBIN_MODEL_PRESET to one of these, or supply "
                f"VARCO_CASBIN_MODEL_PATH / model_text for a custom model."
            )
        # importlib.resources keeps this working from a zipped wheel too.
        preset = resources.files("varco_casbin.models").joinpath(f"{self.model_preset}.conf")
        return preset.read_text(encoding="utf-8")


__all__ = [
    "CasbinSettings",
]
