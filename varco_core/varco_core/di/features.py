"""
varco_core.di.features
========================
``Feature``/``FEATURES`` and ``feature_default()`` — the one
feature-enable convention (Plan 047 §D-features): a binding-shaped
feature is switchable by ``enable_<feature>(container)`` (code) or
``VARCO_<FEATURE>_ENABLED`` (env), code beating env by the same
fallback/priority mechanism that makes backend selection work.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class Feature:
    """
    One switchable feature (§D-features).

    Args:
        feature: Short registry key (``"feature_flags"``, ...).
        env: The ``VARCO_<FEATURE>_ENABLED`` variable name, or ``None``
            for the documented no-env-form exception
            (``authorization_audit``).
        interface_dotted: ``"module.path:ClassName"`` the feature binds,
            for the catalogue — informational only, never imported eagerly.
        category: One of the §D-features matrix categories
            (``"integration"``/``"security"``/``"dev-only"``) — feeds the
            published on/off matrix.
        default_on: Whether the feature is on with no configuration at all.
        shape: ``"binding"`` (an ``enable_*`` swaps a Null default) or
            ``"wrapper"`` (wraps the live binding — no env form is
            possible, see ``authorization_audit``).
        code_verb: The `enable_*` function name, for documentation.
    """

    feature: str
    env: str | None
    interface_dotted: str
    category: str
    default_on: bool
    shape: str
    code_verb: str


#: Per §D-features' "Sites" paragraph.
FEATURES: tuple[Feature, ...] = (
    Feature(
        feature="feature_flags",
        env="VARCO_FEATURE_FLAGS_ENABLED",
        interface_dotted="varco_core.flags.base:AbstractFeatureFlags",
        category="integration",
        default_on=False,
        shape="binding",
        code_verb="enable_feature_flags",
    ),
    Feature(
        feature="tenant_membership",
        env="VARCO_TENANT_MEMBERSHIP_ENABLED",
        interface_dotted="varco_core.tenancy.membership:AbstractTenantMembership",
        category="integration",
        default_on=False,
        shape="binding",
        code_verb="enable_tenant_membership",
    ),
    Feature(
        feature="policy_authorizer",
        env="VARCO_POLICY_AUTHORIZER_ENABLED",
        interface_dotted="varco_core.auth.authorizer:AbstractAuthorizer",
        category="integration",
        default_on=False,
        shape="binding",
        code_verb="enable_policy_authorizer",
    ),
    Feature(
        feature="authorization_audit",
        env=None,
        interface_dotted="varco_core.auth.authorizer:AbstractAuthorizer",
        category="security",
        default_on=False,
        shape="wrapper",
        code_verb="enable_authorization_audit",
    ),
)


def feature_default(feature: str) -> Callable[[Any], Any]:
    """
    Decorator gating a feature's env-driven twin provider on
    ``VARCO_<FEATURE>_ENABLED`` being truthy (§D-features).

    Args:
        feature: The feature's registry key — must exist in ``FEATURES``
            and have a non-``None`` ``env``.

    Returns:
        A decorator applying ``providify.Requires(condition=env_truthy(env))``
        to the target, and mirroring the resulting marker tuple onto
        ``target.__requires__`` (same convenience mirror as
        ``varco_core.di.select.selected_by()``).

    Raises:
        LookupError: ``feature`` is not registered in ``FEATURES``.
        ValueError: The registered feature has no ``env`` (the
            ``authorization_audit`` wrapper-shaped exception) — there is
            nothing to gate on.

    Example::

        @Fallback
        @Provider(singleton=True, priority=BACKEND_PRIORITY)
        @feature_default("feature_flags")
        def feature_flags_when_enabled() -> AbstractFeatureFlags:
            return InMemoryFeatureFlags()
    """
    entry = next((f for f in FEATURES if f.feature == feature), None)
    if entry is None:
        raise LookupError(
            f"Unknown feature {feature!r} — known features: "
            f"{', '.join(f.feature for f in FEATURES)}."
        )
    env = entry.env
    if env is None:
        raise ValueError(
            f"feature_default({feature!r}): this feature has no env form "
            f"(shape={entry.shape!r}) — see §D-features' documented exception."
        )

    def decorator(target: Any) -> Any:
        from providify import Requires  # noqa: PLC0415 — import-budget rule

        from varco_core.di.select import env_truthy  # noqa: PLC0415

        Requires(condition=env_truthy(env))(target)
        target.__requires__ = getattr(target, "__di_requires__", ())
        return target

    return decorator


__all__ = ["Feature", "FEATURES", "feature_default"]
