"""
billing_service
===============
A second minimal standalone varco service for the composite example.

Deliberately reads the **same bare env-var name** as the orders service
(``SERVICE_DB_URL``) to demonstrate the build-time collision hazard that
``build_service`` solves: two services reading the same name in one process
would otherwise see the same value.
"""

from __future__ import annotations

import os

from fastapi import FastAPI
from providify import DIContainer
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.presets import GenericRouter

from varco_fastapi import create_varco_app


def create_billing_app() -> FastAPI:
    """
    Build the billing service standalone.

    Reads ``SERVICE_DB_URL`` — the *same* bare name the orders factory reads in
    the composite — to show that ``build_service``'s scoped env keeps them
    isolated even under identical names.

    Returns:
        A fully configured ``FastAPI`` app for the billing service.
    """
    db_url = os.environ.get("SERVICE_DB_URL", "postgres://localhost/billing")

    class BillingRouter(GenericRouter):
        _prefix = "/billing-api"

        @route("GET", "/status")
        async def status(self) -> dict[str, str]:
            return {"service": "billing", "db": db_url}

    return create_varco_app(
        DIContainer(),
        routers=[BillingRouter],
        title="Billing Service",
        version="1.0.0",
        validate=False,
    )
