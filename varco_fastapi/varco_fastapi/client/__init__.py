"""
varco_fastapi.client
=====================
HTTP client layer for varco services.

Public API::

    from varco_fastapi.client import (
        AsyncVarcoClient,  # alias: VarcoClient
        ClientProfile,
        ClientConfigurator,
        ClientProtocol,
        SyncVarcoClient,
        JobHandle,
        JobFailedError,
        PreparedRequest,
        AbstractClientMiddleware,
        HeadersMiddleware,
        CorrelationIdMiddleware,
        AuthForwardMiddleware,
        JwtMiddleware,
        RetryMiddleware,
        LoggingMiddleware,
        TimeoutMiddleware,
        OTelClientMiddleware,
    )
"""

from __future__ import annotations

from varco_fastapi.client.base import AsyncVarcoClient, ClientProfile, VarcoClient
from varco_fastapi.client.configurator import ClientConfigurator
from varco_fastapi.client.handle import JobFailedError, JobHandle
from varco_fastapi.client.middleware import (
    AbstractClientMiddleware,
    AuthForwardMiddleware,
    CorrelationIdMiddleware,
    HeadersMiddleware,
    JwtMiddleware,
    LoggingMiddleware,
    OTelClientMiddleware,
    PreparedRequest,
    RetryMiddleware,
    TimeoutMiddleware,
)
from varco_fastapi.client.protocol import ClientProtocol
from varco_fastapi.client.sync import SyncClientAsyncError, SyncVarcoClient

__all__ = [
    "AsyncVarcoClient",
    "VarcoClient",
    "ClientProfile",
    "ClientConfigurator",
    "ClientProtocol",
    "SyncVarcoClient",
    "SyncClientAsyncError",
    "JobHandle",
    "JobFailedError",
    "PreparedRequest",
    "AbstractClientMiddleware",
    "HeadersMiddleware",
    "CorrelationIdMiddleware",
    "AuthForwardMiddleware",
    "JwtMiddleware",
    "RetryMiddleware",
    "LoggingMiddleware",
    "TimeoutMiddleware",
    "OTelClientMiddleware",
]
