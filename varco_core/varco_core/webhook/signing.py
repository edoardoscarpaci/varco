"""
varco_core.webhook.signing
============================
``WebhookSigner`` ABC and its two implementations — Standard Webhooks
(default) and RFC 9421 (opt-in) — per §D-D4-signing (Plan 031 / D4b,
Steps 6-9).

§D-D4-signing is the load-bearing design decision this module encodes: a
webhook signature is only worth what the *receiver* can verify, and
Standard Webhooks (not RFC 9421) is what off-the-shelf verification
snippets in the wild actually understand. See the module docstring in
``plans/031-outbound-webhooks.md``'s design section for the full DESIGN
block and rejected alternatives (Stripe's ``t=,v1=`` shape, RFC 9421 only).

Replay protection: both signers reject a timestamp outside a tolerance
window (default 300s, brief 005 §2 — convention, not normative) and sign
the canonical concatenated form rather than a hand-built string, which is
what prevents extension attacks.

Thread safety:  ✅ Signers are stateless aside from their (immutable)
                   secret list — safe to share across requests.
Async safety:   ✅ All signing/verification here is pure CPU (HMAC) — no
                   ``async def`` needed.
"""

from __future__ import annotations

import abc
import base64
import hashlib
import hmac
import time
from dataclasses import dataclass

__all__ = [
    "WebhookSigner",
    "StandardWebhooksSigner",
    "Rfc9421Signer",
    "get_signer",
]

_DEFAULT_TOLERANCE_SECONDS = 300.0


class WebhookSigner(abc.ABC):
    """
    Common contract for a webhook signing scheme.

    Each implementation defines its own ``sign``/``verify`` signature
    (they carry genuinely different inputs — Standard Webhooks needs
    ``msg_id``/``timestamp``/``payload``; RFC 9421 needs the HTTP request
    line components too) so this ABC only fixes the *shape*: both accept
    a list of active secrets at construction and both produce/consume a
    ``dict[str, str]`` of headers.

    Thread safety:  ✅ Stateless beyond the immutable secret list.
    Async safety:   ✅ Pure CPU — no I/O.
    """

    def __init__(
        self, secrets: list[str], *, tolerance_seconds: float = _DEFAULT_TOLERANCE_SECONDS
    ) -> None:
        if not secrets:
            raise ValueError(f"{type(self).__name__} requires at least one secret.")
        self._secrets = list(secrets)
        self._tolerance_seconds = tolerance_seconds

    @abc.abstractmethod
    def sign(self, **kwargs: object) -> dict[str, str]:
        """Produce the scheme's headers for one outgoing delivery."""
        raise NotImplementedError

    @abc.abstractmethod
    def verify(self, **kwargs: object) -> bool:
        """Verify a delivery's headers against this signer's secrets."""
        raise NotImplementedError


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _b64d(data: str) -> bytes:
    return base64.b64decode(data)


class StandardWebhooksSigner(WebhookSigner):
    """
    Standard Webhooks HMAC-SHA256 signer — the shipped **default**
    (§D-D4-signing).

    Signs ``{msg_id}.{timestamp}.{payload}`` (the canonical concatenated
    form the spec defines — never a hand-built alternative, which is what
    prevents extension attacks) with HMAC-SHA256, keyed by the secret's
    payload after stripping the conventional ``whsec_`` prefix and
    base64-decoding it if it looks base64-encoded. To keep this
    implementation dependency-free and match the published known-answer
    vector, the secret bytes are used directly (UTF-8) when they do not
    decode as base64 — see ``_secret_bytes``.

    Emits ``webhook-id``, ``webhook-timestamp``, ``webhook-signature``
    (space-delimited ``v1,<base64 sig>`` — one per active secret, giving
    zero-downtime rotation natively).

    Thread safety:  ✅ Stateless beyond the immutable secret list.
    Async safety:   ✅ Pure CPU.
    """

    def _secret_bytes(self, secret: str) -> bytes:
        # Standard Webhooks secrets are conventionally "whsec_" + base64.
        # Strip the prefix and base64-decode; fall back to raw UTF-8 bytes
        # for a secret that does not follow the convention (e.g. a plain
        # rotation-test string in our own test suite).
        raw = secret[len("whsec_") :] if secret.startswith("whsec_") else secret
        try:
            return _b64d(raw)
        except Exception:  # noqa: BLE001 - deliberate fallback, not a real error path
            return secret.encode("utf-8")

    def _sign_one(self, secret: str, signed_content: bytes) -> str:
        digest = hmac.new(self._secret_bytes(secret), signed_content, hashlib.sha256).digest()
        return f"v1,{_b64(digest)}"

    def sign(self, *, msg_id: str, timestamp: str, payload: str | bytes) -> dict[str, str]:  # type: ignore[override]
        """
        Produce ``webhook-id``/``webhook-timestamp``/``webhook-signature``.

        Args:
            msg_id:    Stable delivery id (the receiver's dedup key).
            timestamp: Unix timestamp (seconds, as a string).
            payload:   The exact JSON body being sent. ``str`` or ``bytes``
                       (Plan 038 / S19, §D-S19-gap) — widened additively so
                       a non-UTF-8 body never needs a lossy decode. Byte-
                       identical to the pre-widening ``str``-only behaviour
                       for any valid-UTF-8 ``str`` input.

        Returns:
            The three Standard Webhooks headers. ``webhook-signature`` is
            space-delimited, one ``v1,<sig>`` entry per active secret.
        """
        # DESIGN (§D-S19-gap, brief 013 §50.2): build the prefix as bytes and
        # concatenate raw payload bytes directly rather than
        # f"{...}.{payload}".encode() — the latter requires payload to be a
        # str, forcing a decode of a raw inbound body that may not be valid
        # UTF-8. ✅ byte-identical output for a str payload (proved by Step 3's
        # round-trip test). ❌ one extra branch versus a single f-string.
        prefix = f"{msg_id}.{timestamp}.".encode()
        payload_bytes = payload if isinstance(payload, bytes) else payload.encode()
        signed_content = prefix + payload_bytes
        signatures = [self._sign_one(secret, signed_content) for secret in self._secrets]
        return {
            "webhook-id": msg_id,
            "webhook-timestamp": timestamp,
            "webhook-signature": " ".join(signatures),
        }

    def verify(self, *, payload: str | bytes, headers: dict[str, str]) -> bool:  # type: ignore[override]
        """
        Verify ``headers['webhook-signature']`` against this signer's
        secrets, using a constant-time comparison and rejecting a
        timestamp outside the tolerance window.

        Args:
            payload: The exact body that was signed. ``str`` or ``bytes``
                     (§D-S19-gap) — pass ``bytes`` for a raw inbound body
                     that may not be valid UTF-8.
            headers: The Standard Webhooks headers to verify.

        Returns:
            ``True`` iff the timestamp is within tolerance AND at least one
            signature in the (possibly space-delimited) header matches one
            of this signer's active secrets.
        """
        try:
            msg_id = headers["webhook-id"]
            timestamp = headers["webhook-timestamp"]
            signature_header = headers["webhook-signature"]
        except KeyError:
            return False

        try:
            ts = float(timestamp)
        except ValueError:
            return False
        if abs(time.time() - ts) > self._tolerance_seconds:
            return False

        prefix = f"{msg_id}.{timestamp}.".encode()
        payload_bytes = payload if isinstance(payload, bytes) else payload.encode()
        signed_content = prefix + payload_bytes
        candidates = [self._sign_one(secret, signed_content) for secret in self._secrets]
        provided = signature_header.split(" ")

        for provided_sig in provided:
            for candidate in candidates:
                if hmac.compare_digest(provided_sig, candidate):
                    return True
        return False


@dataclass(frozen=True)
class _Rfc9421SignatureBase:
    """Structured components covered by an RFC 9421 signature base — built
    field-by-field, never via string concatenation (§D-D4-signing:
    "never string concatenation... prevents extension attacks")."""

    method: str
    target_uri: str
    authority: str
    content_digest: str
    created: int
    keyid: str
    alg: str = "hmac-sha256"

    def covered_components(self) -> tuple[str, ...]:
        return ("@method", "@target-uri", "@authority", "content-digest")

    def signature_input_value(self) -> str:
        components = " ".join(f'"{c}"' for c in self.covered_components())
        return f'({components});created={self.created};keyid="{self.keyid}";alg="{self.alg}"'

    def signature_base(self) -> bytes:
        lines = [
            f'"@method": {self.method}',
            f'"@target-uri": {self.target_uri}',
            f'"@authority": {self.authority}',
            f'"content-digest": {self.content_digest}',
            f'"@signature-params": {self.signature_input_value()}',
        ]
        return "\n".join(lines).encode()


class Rfc9421Signer(WebhookSigner):
    """
    RFC 9421 "HTTP Message Signatures" signer, opt-in (§D-D4-signing).

    Covers ``@method``, ``@target-uri``, ``@authority``, ``content-digest``
    (RFC 9530, computed here as ``sha-256=:<base64 sha256 of body>:``).
    ``alg`` is fixed to ``hmac-sha256`` (brief 005 §2's registered
    algorithm to start with). The signature base is built field-by-field
    via ``_Rfc9421SignatureBase``, never by concatenating a hand-built
    string, which is what closes the extension-attack class RFC 9421
    itself exists to prevent.

    Args:
        secrets: Base64-encoded HMAC shared secrets, newest last (rotation,
                 same convention as ``StandardWebhooksSigner``).
        keyid:   The ``keyid`` signature parameter identifying which key
                 signed — required so a multi-key receiver knows which
                 secret to verify with.

    Thread safety:  ✅ Stateless beyond the immutable secret list.
    Async safety:   ✅ Pure CPU.
    """

    def __init__(
        self,
        secrets: list[str],
        *,
        keyid: str,
        tolerance_seconds: float = _DEFAULT_TOLERANCE_SECONDS,
    ) -> None:
        super().__init__(secrets, tolerance_seconds=tolerance_seconds)
        self._keyid = keyid

    @staticmethod
    def _content_digest(payload: bytes) -> str:
        digest = hashlib.sha256(payload).digest()
        return f"sha-256=:{_b64(digest)}:"

    def _sign_one(self, secret_b64: str, base: bytes) -> bytes:
        key = _b64d(secret_b64)
        return hmac.new(key, base, hashlib.sha256).digest()

    def sign(  # type: ignore[override]
        self,
        *,
        method: str,
        target_uri: str,
        authority: str,
        payload: bytes,
        created: int | None = None,
    ) -> dict[str, str]:
        """
        Produce ``Signature-Input``, ``Signature``, ``Content-Digest``.

        Args:
            method:     HTTP method (e.g. ``"POST"``).
            target_uri: Full request target URI.
            authority:  The request's ``Host``/authority.
            payload:    Raw request body bytes.
            created:    Unix timestamp for the ``created`` parameter.
                        Defaults to now.
        """
        created = created if created is not None else int(time.time())
        content_digest = self._content_digest(payload)
        base_components = _Rfc9421SignatureBase(
            method=method,
            target_uri=target_uri,
            authority=authority,
            content_digest=content_digest,
            created=created,
            keyid=self._keyid,
        )
        # Newest (last) secret signs outgoing requests — rotation for RFC
        # 9421 is "roll keyid forward", not multi-signature like Standard
        # Webhooks (RFC 9421 has one Signature per Signature-Input entry;
        # multiple secrets are for *verifying* older still-valid deliveries
        # only, not for signing every one of them at once).
        sig = self._sign_one(self._secrets[-1], base_components.signature_base())
        return {
            "Signature-Input": f"sig1={base_components.signature_input_value()}",
            "Signature": f"sig1=:{_b64(sig)}:",
            "Content-Digest": content_digest,
        }

    def verify(  # type: ignore[override]
        self,
        *,
        method: str,
        target_uri: str,
        authority: str,
        payload: bytes,
        headers: dict[str, str],
    ) -> bool:
        """
        Verify ``headers['Signature']`` against this signer's secrets.

        Returns:
            ``True`` iff the ``created`` parameter is within tolerance and
            the signature matches for at least one active secret.
        """
        try:
            signature_input = headers["Signature-Input"]
            signature = headers["Signature"]
            content_digest = headers["Content-Digest"]
        except KeyError:
            return False

        expected_digest = self._content_digest(payload)
        if not hmac.compare_digest(content_digest, expected_digest):
            return False

        created = _extract_param(signature_input, "created")
        keyid = _extract_param(signature_input, "keyid", quoted=True)
        alg = _extract_param(signature_input, "alg", quoted=True) or "hmac-sha256"
        if created is None:
            return False
        if abs(time.time() - int(created)) > self._tolerance_seconds:
            return False

        base_components = _Rfc9421SignatureBase(
            method=method,
            target_uri=target_uri,
            authority=authority,
            content_digest=content_digest,
            created=int(created),
            keyid=keyid or self._keyid,
            alg=alg,
        )
        provided_sig_b64 = _extract_sig_value(signature)
        if provided_sig_b64 is None:
            return False
        provided_sig = _b64d(provided_sig_b64)

        base = base_components.signature_base()
        for secret in self._secrets:
            candidate = self._sign_one(secret, base)
            if hmac.compare_digest(provided_sig, candidate):
                return True
        return False


def _extract_param(signature_input: str, name: str, *, quoted: bool = False) -> str | None:
    """Pull ``name=value`` (optionally quoted) out of a Signature-Input value."""
    import re

    pattern = rf'{name}="([^"]*)"' if quoted else rf"{name}=(\d+)"
    match = re.search(pattern, signature_input)
    return match.group(1) if match else None


def _extract_sig_value(signature_header: str) -> str | None:
    """Pull the ``:<base64>:`` payload out of ``sig1=:<base64>:``."""
    import re

    match = re.search(r":([A-Za-z0-9+/=]+):", signature_header)
    return match.group(1) if match else None


def get_signer(scheme_name: str, *, secrets: list[str], **kwargs: object) -> WebhookSigner:
    """
    Construct a ``WebhookSigner`` by scheme name (§D-D4-signing dispatch).

    Args:
        scheme_name: ``"standard_webhooks"`` or ``"rfc9421"`` — matches
                     ``WebhookSubscription.signer``.
        secrets:     Active secrets, newest last.
        **kwargs:    Forwarded to the implementation's constructor (e.g.
                     ``keyid=`` for ``Rfc9421Signer``).

    Raises:
        ValueError: Unknown ``scheme_name``.
    """
    if scheme_name == "standard_webhooks":
        return StandardWebhooksSigner(secrets, **kwargs)  # type: ignore[arg-type]
    if scheme_name == "rfc9421":
        kwargs.setdefault("keyid", "default")
        return Rfc9421Signer(secrets, **kwargs)  # type: ignore[arg-type]
    raise ValueError(f"Unknown webhook signing scheme: {scheme_name!r}")
