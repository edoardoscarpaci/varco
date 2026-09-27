"""
varco_casbin.modules
=======================
``CasbinModule`` — ``varco_casbin``'s one ``VarcoModule`` (Plan 049 / DI-1,
Step 8).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from varco_core.di.module import CoreModule, VarcoModule

if TYPE_CHECKING:
    from providify import DIContainer


class CasbinModule(VarcoModule):
    """
    Registers ``varco_casbin``'s ``@Fallback`` defaults (``bootstrap()``).

    Args:
        policy_authorizer: ``True`` calls ``enable_policy_authorizer()``
            (a code decision — always on). ``None`` (default) leaves 047's
            env twin (``VARCO_POLICY_AUTHORIZER_ENABLED``) in charge.
            ``False`` binds nothing extra.
    """

    name = "casbin"
    depends_on = (CoreModule,)

    def __init__(
        self, *, policy_authorizer: bool | None = None, enabled: bool | None = None
    ) -> None:
        super().__init__(enabled=enabled)
        self._policy_authorizer = policy_authorizer

    def _register(self, container: DIContainer) -> None:
        from varco_casbin.di import bootstrap, enable_policy_authorizer  # noqa: PLC0415

        bootstrap(container)
        if self._policy_authorizer is True:
            enable_policy_authorizer(container)


__all__ = ["CasbinModule"]
