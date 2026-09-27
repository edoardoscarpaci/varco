"""
orders_service
==============
A minimal standalone varco service standing in for a real microservice.

In a real deployment this would be its own package with its own
``create_varco_app()`` call, its own ``DIContainer``, and its own database.
Here it is intentionally tiny — a single ``GenericRouter`` route plus a
config value read from the environment — so the example stays focused on the
*composition*, not on any one service's internals.

The ``ORDERS_DB_URL`` read in ``create_orders_app`` demonstrates per-service
configuration: when combined via ``build_service`` (see ``composite.py``), each
service is built under its own scoped environment.
"""

from __future__ import annotations

import os

from fastapi import FastAPI
from providify import DIContainer
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.presets import GenericRouter

from varco_fastapi import create_varco_app


def create_orders_app() -> FastAPI:
    """
    Build the orders service exactly as it would be built standalone.

    Reads ``ORDERS_DB_URL`` from the environment at build time to prove that the
    service captures its own configuration (the composite never shares it).

    Returns:
        A fully configured ``FastAPI`` app for the orders service.
    """
    # Read config at build time — in the composite this is scoped per-service.
    db_url = os.environ.get("ORDERS_DB_URL", "postgres://localhost/orders")

    class OrdersRouter(GenericRouter):
        _prefix = "/orders-api"

        @route("GET", "/status")
        async def status(self) -> dict[str, str]:
            # Echoes the DB URL this service was built with, to make isolation
            # visible when you hit /orders/orders-api/status in the composite.
            return {"service": "orders", "db": db_url}

    return create_varco_app(
        DIContainer(),
        routers=[OrdersRouter],
        title="Orders Service",
        version="1.0.0",
        validate=False,
    )
