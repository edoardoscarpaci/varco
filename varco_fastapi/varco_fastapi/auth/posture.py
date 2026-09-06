"""
varco_fastapi.auth.posture
=============================

``inspect_auth_posture()`` — a **pure, read-only** introspection function
over an ``AbstractServerAuth`` tree (Plan 034 / Phase 4, §D-034-seam).

Reports facts. Plan 036 owns the judgement, the thresholds, and the
startup wiring — do not add a warning, a raise, or a lifespan hook here.
This module mirrors 037's ``inspect_rls_posture()`` precedent
(``varco_sa``): each source plan exports its own typed report in its own
package; 036 aggregates them.

Thread safety:  ✅ Pure function — no shared state, no I/O.
Async safety:   ✅ Synchronous, no I/O — safe to call from anywhere.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from varco_fastapi.auth.server_auth import (
    ApiKeyAuth,
    CompositeServerAuth,
    PassthroughAuth,
    WebSocketAuth,
)

if TYPE_CHECKING:
    from varco_fastapi.auth.server_auth import AbstractServerAuth

__all__ = ["AuthPostureReport", "inspect_auth_posture"]


@dataclass(frozen=True)
class AuthPostureReport:
    """
    A snapshot of facts about an ``AbstractServerAuth`` tree.

    Attributes:
        components:                          Class names walked, in the
                                              order visited (root first).
        api_key_query_fallback_enabled:      ``True`` if any ``ApiKeyAuth``
                                              in the tree has a non-``None``
                                              ``param``.
        api_key_query_param_name:            The first such ``param`` name
                                              found, or ``None``.
        api_key_plaintext_source:            ``True`` if any ``ApiKeyAuth``
                                              in the tree was constructed
                                              with ``keys=`` (plaintext)
                                              rather than ``hashed_keys=``.
        api_key_pepper_configured:           ``True`` if any ``ApiKeyAuth``
                                              in the tree has a non-``None``
                                              pepper configured.
        websocket_token_query_fallback_enabled: ``True`` if any
                                              ``WebSocketAuth`` in the tree
                                              has a non-``None``
                                              ``token_query_param``.
        passthrough_auth_bound:              ``True`` if a ``PassthroughAuth``
                                              is present anywhere in the tree
                                              (nested inside a composite or a
                                              ``WebSocketAuth``, or bare).

    ⚠️ ``passthrough_auth_bound`` reports **presence, not publicness** —
    whether the app is "public" is not knowable from an auth object alone;
    that judgement belongs to Plan 036.
    """

    components: tuple[str, ...]
    api_key_query_fallback_enabled: bool
    api_key_query_param_name: str | None
    api_key_plaintext_source: bool
    api_key_pepper_configured: bool
    websocket_token_query_fallback_enabled: bool
    passthrough_auth_bound: bool


def inspect_auth_posture(auth: AbstractServerAuth) -> AuthPostureReport:
    """
    Walk an ``AbstractServerAuth`` tree and report facts about it.

    Recurses into ``CompositeServerAuth.strategies`` and
    ``WebSocketAuth.inner`` so a wrapped/nested auth strategy (e.g. a
    ``PassthroughAuth`` inside a ``CompositeServerAuth`` inside a
    ``WebSocketAuth``) is still reported.

    Args:
        auth: The root ``AbstractServerAuth`` to inspect.

    Returns:
        An ``AuthPostureReport``. Never raises — an unrecognized
        ``AbstractServerAuth`` subclass is simply recorded in
        ``components`` and contributes nothing else.

    Edge cases:
        - A cyclic composite (an auth object containing itself) terminates
          via an identity-set guard rather than recursing forever.
        - Never logs — this is a pure read, not a diagnostic action.

    Example::

        report = inspect_auth_posture(my_app_auth)
        if report.api_key_query_fallback_enabled:
            ...  # Plan 036's judgement, not this function's
    """
    components: list[str] = []
    api_key_query_fallback_enabled = False
    api_key_query_param_name: str | None = None
    api_key_plaintext_source = False
    api_key_pepper_configured = False
    websocket_token_query_fallback_enabled = False
    passthrough_auth_bound = False

    visited: set[int] = set()

    def _walk(node: object) -> None:
        nonlocal \
            api_key_query_fallback_enabled, \
            api_key_query_param_name, \
            api_key_plaintext_source, \
            api_key_pepper_configured, \
            websocket_token_query_fallback_enabled, \
            passthrough_auth_bound

        # Identity-set guard — a cyclic composite must terminate rather
        # than recurse forever (Edge cases above).
        node_id = id(node)
        if node_id in visited:
            return
        visited.add(node_id)

        components.append(type(node).__name__)

        if isinstance(node, ApiKeyAuth):
            if node._param is not None:
                api_key_query_fallback_enabled = True
                if api_key_query_param_name is None:
                    api_key_query_param_name = node._param
            # `keys=` and `hashed_keys=` both populate the same internal
            # `_digest_keys` dict (§D-S14-hash's raw-key-never-survives
            # guarantee), so ApiKeyAuth records which constructor path was
            # used in a dedicated `_constructed_from_plaintext` flag purely
            # for read-only reporting like this — it plays no role in any
            # auth decision.
            api_key_plaintext_source = api_key_plaintext_source or node._constructed_from_plaintext
            if node._pepper is not None:
                api_key_pepper_configured = True

        if isinstance(node, PassthroughAuth):
            passthrough_auth_bound = True

        if isinstance(node, WebSocketAuth):
            if node._token_query_param is not None:
                websocket_token_query_fallback_enabled = True
            _walk(node._inner)

        if isinstance(node, CompositeServerAuth):
            for strategy in node._strategies:
                _walk(strategy)

    _walk(auth)

    return AuthPostureReport(
        components=tuple(components),
        api_key_query_fallback_enabled=api_key_query_fallback_enabled,
        api_key_query_param_name=api_key_query_param_name,
        api_key_plaintext_source=api_key_plaintext_source,
        api_key_pepper_configured=api_key_pepper_configured,
        websocket_token_query_fallback_enabled=websocket_token_query_fallback_enabled,
        passthrough_auth_bound=passthrough_auth_bound,
    )
