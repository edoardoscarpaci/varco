"""
varco_core.exception.settings
================================
``ErrorEnvelopeSettings`` — Plan 011 D-4's kill switch.

Upgrading gives a **built-in** varco exception's JSON body up to two new
keys, ``message_key`` and ``params`` (D-4, the plan's one deliberate wire
delta). Set both flags below to ``False`` to restore the exact pre-plan
body for every exception, in one env var each:

    VARCO_ERROR_INCLUDE_MESSAGE_KEY=false
    VARCO_ERROR_INCLUDE_PARAMS=false
"""

from __future__ import annotations

from pydantic_settings import SettingsConfigDict

from varco_core.config import VarcoSettings

__all__ = ["ErrorEnvelopeSettings"]


class ErrorEnvelopeSettings(VarcoSettings):
    """
    Controls what the error envelope emits.

    Attributes:
        include_message_key: Emit ``message_key`` on built-in exceptions
            (D-4). Default ``True``.
        include_params: Emit non-empty ``params`` on built-in exceptions
            (D-4). Default ``True``.
        problem_details: Switch to the RFC 9457
            ``application/problem+json`` media type and emit
            ``type``/``title``/``detail``/``instance`` (D-3). Default
            ``False`` — Spring Boot's precedent for an additive, opt-in
            rollout.
        problem_type_base: Base URI prepended to ``message_key`` to build
            the RFC 9457 ``type`` member, e.g.
            ``"https://errors.example.com/"`` +
            ``"varco.error.not_found"``. ``None`` uses ``"about:blank"``.
        include_detail: Emit the ``detail`` member (``str(exc)``) on the
            error envelope. Default ``True`` — byte-identical to pre-3.2
            behaviour (Plan 035 / §D-S3b). This is a **warn-only** knob in
            3.2: ``detail`` is deliberately still present by default because
            it is the only actionable channel for e.g. ``RouteGuard`` denial
            messages (``exceptions.py:135-141``); ``inspect_http_edge()``
            reports ``http.error.detail_exposed`` so an operator can flip it
            per-deployment. **4.0 flip candidate** — the default is planned
            to become ``False`` in varco 4.0.
        redact_params: Plan 040 / S21, §D-S21-errparams. Route
            ``error_params()``'s return value through
            ``varco_core.redaction.redact_mapping()`` (secret-named keys ->
            ``"[REDACTED]"``) and ``json_safe()`` (non-JSON values ->
            ``"<TypeName>"``) before emitting them on the envelope. Default
            ``True`` — byte-identical for every in-tree ``ServiceException``
            (none of their params match a redaction pattern or carry a
            non-JSON value); changes only an out-of-tree exception whose
            ``error_params()`` returns a secret-named key or a live object.
            **Does not** catch a secret *value* under a benign key — see
            ``ServiceException.error_params()``'s docstring for the
            documented residue. ``VARCO_ERROR_REDACT_PARAMS=false`` restores
            the pre-3.2 body byte-for-byte.
    """

    model_config = SettingsConfigDict(env_prefix="VARCO_ERROR_", extra="ignore")

    include_message_key: bool = True
    include_params: bool = True
    problem_details: bool = False
    problem_type_base: str | None = None
    include_detail: bool = True
    redact_params: bool = True
