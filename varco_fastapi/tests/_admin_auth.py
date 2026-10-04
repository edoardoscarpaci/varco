"""
Shared ``server_auth`` stand-ins for the admin-surface tests (Plan 054 /
FLIP-3/4).

Since 4.0 ``mount_reliability_admin``/``mount_webhook_admin`` and the three
admin router builders require ``server_auth`` (``None`` → ``ValueError``) and
enforce ``admin_role`` on every route, so every test that builds one needs a
stub. One home for both shapes:

* `ctx_auth` — a zero-argument callable resolving to a ctx with fixed
  ``roles`` (moved from ``test_admin_tenant_guard.py``'s ``_CtxAuth``).
* `HeaderAuth` — a ``Request``-taking callable that raises ``401`` without an
  ``X-Test-Auth`` header and otherwise resolves to a ctx whose roles are the
  header's comma-separated value; counts its own invocations.

Not a pytest fixture module — plain helpers, imported explicitly.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException, Request

__all__ = ["HEADER", "CtxAuth", "HeaderAuth", "RolesCtx", "ctx_auth"]

#: The header `HeaderAuth` reads its roles from.
HEADER = "X-Test-Auth"


class RolesCtx:
    """
    A minimal auth context exposing ``roles`` — all the admin routers read.

    Args:
        roles: The roles the caller holds.
    """

    def __init__(self, roles: list[str]) -> None:
        self.roles = list(roles)


class CtxAuth:
    """
    A zero-argument ``server_auth`` resolving to a fixed set of roles.

    Args:
        roles: The roles every request resolves to.
    """

    def __init__(self, roles: list[str]) -> None:
        self._roles = list(roles)

    def __call__(self) -> RolesCtx:
        """
        Returns:
            A fresh `RolesCtx` carrying the configured roles.
        """
        return RolesCtx(self._roles)


def ctx_auth(roles: list[str]) -> CtxAuth:
    """
    Build a `CtxAuth` for *roles*.

    Args:
        roles: The roles every request resolves to.

    Returns:
        A ``server_auth`` callable.

    Example::

        mount_reliability_admin(app, dlq=dlq, acknowledge_bundled_admin=True,
                                server_auth=ctx_auth(["reliability-admin"]))
    """
    return CtxAuth(roles)


class HeaderAuth:
    """
    A ``Request``-taking ``server_auth``: 401 without `HEADER`, otherwise a
    `RolesCtx` whose roles are the header's comma-separated value.

    Attributes:
        calls: How many times this strategy was invoked — lets a test assert
            ``server_auth`` is resolved once per request.
    """

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, request: Request) -> Any:
        """
        Args:
            request: The incoming request.

        Returns:
            A `RolesCtx` for the header's roles.

        Raises:
            HTTPException: 401 when `HEADER` is absent.
        """
        self.calls += 1
        raw = request.headers.get(HEADER)
        if raw is None:
            raise HTTPException(status_code=401, detail="stub: no credentials")
        return RolesCtx([r for r in raw.split(",") if r])
