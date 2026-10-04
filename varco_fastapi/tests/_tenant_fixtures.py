"""
Shared tenant-resolution test helpers (Plan 055 / FLIP-6, Step 9).

Since 4.0 ``TenantResolutionMiddleware(chain=None)`` with nothing configured
is the deprecated implicit path, and ``varco_fastapi``'s pytest config turns
its warning into an error (``error:TenantResolutionMiddleware\\(chain=None\\)``).
Every test that is *not* about that path names header-only resolution
explicitly through :func:`legacy_chain`, the permanent escape hatch.

Not a pytest fixture module — plain helpers, imported explicitly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from varco_core.tenancy.source import TenantSourceChain


def legacy_chain(header: str = "X-Tenant-Id") -> TenantSourceChain:
    """
    Build the explicit header-only chain — ``LegacyTenantSource(header)``.

    Args:
        header: The header the source reads. Defaults to ``X-Tenant-Id``.

    Returns:
        A one-source ``TenantSourceChain``; constructing it never warns.
    """
    from varco_core.tenancy.source import TenantSourceChain  # noqa: PLC0415
    from varco_core.tenancy.sources import LegacyTenantSource  # noqa: PLC0415

    return TenantSourceChain(sources=(LegacyTenantSource(header),))
