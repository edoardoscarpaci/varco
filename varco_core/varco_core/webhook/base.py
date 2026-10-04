"""
varco_core.webhook.base
========================
``WebhookSubscriptionRepository`` — the storage contract for
``WebhookSubscription`` (Plan 031 / D4a, Step 1, §D-D4-entity), plus
``InMemoryWebhookSubscriptionRepository``, the default single-process
implementation used by unit tests and the fast dev path.

DESIGN: a dedicated ABC over reusing ``AsyncRepository[WebhookSubscription]``
    ✅ ``WebhookSubscription`` is a framework-owned table (like the DLQ, the
       outbox, the idempotency store) with exactly two access patterns the
       dispatcher/admin surface actually need — by id and by tenant — not
       the full generic-CRUD-plus-query-AST surface ``AsyncRepository``
       exposes for application entities.
    ✅ Keeps ``varco_sa``/``varco_beanie`` implementations symmetrical with
       every other framework table (``SAIdempotencyStore``, ``SADlq``, …):
       own table, own ``MetaData``, ``register_framework_metadata()``, a
       manual dataclass↔row mapping — never the ``@register`` ORM
       generator.
    ❌ A second repository shape in the codebase — accepted; the framework
       table convention already has ten precedents.

Thread safety:  ⚠️ Implementation-defined — see each concrete subclass.
Async safety:   ✅ All methods are ``async def``.
"""

from __future__ import annotations

import abc
import asyncio
from copy import deepcopy
from typing import TYPE_CHECKING, TypeVar
from uuid import uuid4

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from varco_core.encryption import FieldEncryptor
    from varco_core.webhook.models import WebhookSubscription

__all__ = [
    "WebhookSubscriptionRepository",
    "InMemoryWebhookSubscriptionRepository",
    "require_field_encryptor",
    "reencrypt_legacy_secret",
    "plan_legacy_reencryption",
    "AmbiguousLegacySecretError",
]

#: A subscription identifier as the caller stores it (a ``UUID``, a ``str``).
_IdT = TypeVar("_IdT")

#: The one refusal text both durable repositories raise (Plan 056 /
#: FLIP-13, §D-flip13-refuse) — a single constant so the SA and Beanie
#: messages cannot drift. ``{cls}``: the repository class name; ``{got}``:
#: the type name of what was passed instead of a ``FieldEncryptor``.
_ENCRYPTOR_REQUIRED = (
    "{cls}: encryptor must be a FieldEncryptor (got {got}) — webhook signing "
    "secrets are never stored in plaintext (4.0, Plan 056 / FLIP-13).\n"
    "  Production:  encryptor=FernetFieldEncryptor(key)   "
    "# key = Fernet.generate_key(), kept in your secret store\n"
    "  DI (Plan 050): bind_instance(container, FieldEncryptor, FernetFieldEncryptor(key)) "
    "— the webhook_repository provider injects it\n"
    "  Dev/test only: encryptor=PlaintextFieldEncryptor(acknowledge_plaintext=True)   "
    "# reported as posture.webhook_secrets_plaintext\n"
    "  Existing 3.x plaintext rows: await repo.reencrypt_legacy_plaintext() once after upgrading."
)


def require_field_encryptor(encryptor: object, *, cls_name: str) -> FieldEncryptor:
    """
    Refuse anything that is not a ``FieldEncryptor`` (Plan 056 / FLIP-13).

    Shared by ``SAWebhookSubscriptionRepository`` and
    ``BeanieWebhookSubscriptionRepository`` so the check and its message
    exist once.

    DESIGN: ``TypeError``, via a structural ``isinstance`` check
        ✅ ``FieldEncryptor`` is a ``runtime_checkable`` Protocol, so ``None``
           *and* the second real mistake — passing the Fernet **key**
           (``bytes``) instead of the encryptor — fail with the same message.
        ✅ The annotation (``FieldEncryptor``, not ``| None``) and the runtime
           agree — Plan 053's "a value the annotation forbids is a type
           error" reasoning.
        ❌ ``TypeError`` for a ``None`` value is unusual next to the older
           ``acknowledge_*`` ``ValueError``s; accepted for the reason above.

    Args:
        encryptor: What the caller passed as ``encryptor=``.
        cls_name:  The repository class name, for the message.

    Returns:
        ``encryptor``, narrowed to ``FieldEncryptor``.

    Raises:
        TypeError: ``encryptor`` does not satisfy ``FieldEncryptor``.
    """
    from varco_core.encryption import FieldEncryptor  # noqa: PLC0415 — import budget

    if not isinstance(encryptor, FieldEncryptor):
        raise TypeError(_ENCRYPTOR_REQUIRED.format(cls=cls_name, got=type(encryptor).__name__))
    return encryptor


class AmbiguousLegacySecretError(ValueError):
    """
    A stored webhook secret failed to decrypt and could be read two ways
    (Plan 056 / §D-flip13-reencrypt, drift decision) — raised under the
    default ``legacy_hex=None`` instead of guessing.

    The entry is valid hex decoding to printable UTF-8, so it is either a
    ``PlaintextFieldEncryptor`` row (hex of the secret → read with
    ``legacy_hex=True``) or a raw 3.x secret that is itself a hex string
    (``secrets.token_hex()`` → read with ``legacy_hex=False``). A real
    ciphertext read with the **wrong key** (or a destroyed ``kid``) has the
    same shape — the stored hex of a Fernet token decodes to printable
    base64url — which is why no default reading is safe.

    A ``ValueError`` subclass, so a caller already catching the
    ``legacy_hex=True`` ``ValueError`` keeps working.

    Args:
        subscription_ids: The ambiguous subscriptions, in scan order —
            empty when raised for a single entry by
            ``reencrypt_legacy_secret`` (which does not know the id).

    Attributes:
        subscription_ids: As passed, as a tuple of ``str``.
    """

    def __init__(self, subscription_ids: Sequence[object] = ()) -> None:
        self.subscription_ids: tuple[str, ...] = tuple(str(i) for i in subscription_ids)
        where = (
            f" Subscription id(s): {', '.join(self.subscription_ids)}."
            if self.subscription_ids
            else ""
        )
        super().__init__(
            "reencrypt_legacy_plaintext(): stored webhook secret(s) failed to decrypt under "
            "this repository's encryptor and are ambiguous — valid hex decoding to printable "
            "UTF-8 is either a PlaintextFieldEncryptor row or a raw 3.x secret that is itself "
            "a hex string (and real ciphertext read with the wrong key looks the same)."
            f"{where} Nothing was written. First verify this encryptor decrypts a known-good "
            "row, then re-run with legacy_hex=True (rows written by PlaintextFieldEncryptor) "
            "or legacy_hex=False (raw 3.x secrets, e.g. secrets.token_hex())."
        )


def _is_ambiguous(stored: str) -> bool:
    """
    Whether a non-ciphertext entry has two plausible readings.

    Args:
        stored: The stored ``active_secrets`` entry.

    Returns:
        ``True`` when ``stored`` is valid hex whose bytes decode to
        printable UTF-8 — readable both as hex (``PlaintextFieldEncryptor``)
        and verbatim (a raw 3.x hex secret).
    """
    try:
        return bytes.fromhex(stored).decode("utf-8").isprintable()
    except ValueError:  # UnicodeDecodeError is a ValueError
        return False


def _legacy_plaintext(stored: str, legacy_hex: bool | None) -> str:
    """
    Recover the secret from a stored entry that is not ciphertext.

    Args:
        stored:     The stored ``active_secrets`` entry.
        legacy_hex: ``True`` — the entry is hex of UTF-8 (written by
                    ``PlaintextFieldEncryptor``); ``False`` — the entry is the
                    raw 3.x secret; ``None`` — the entry must be unambiguous
                    (see ``reencrypt_legacy_secret``).

    Returns:
        The secret, as text.

    Raises:
        ValueError: ``legacy_hex=True`` and the entry is not hex of UTF-8.
        AmbiguousLegacySecretError: ``legacy_hex=None`` and the entry is
            valid hex of printable UTF-8.
    """
    if legacy_hex is False:
        return stored
    if legacy_hex:
        return bytes.fromhex(stored).decode("utf-8")
    if _is_ambiguous(stored):
        raise AmbiguousLegacySecretError()
    # Not hex of printable text (e.g. "whsec_…"): only the raw reading exists.
    return stored


def reencrypt_legacy_secret(
    stored: str, encryptor: FieldEncryptor, *, legacy_hex: bool | None = None
) -> str | None:
    """
    Upgrade one stored webhook secret to ciphertext under ``encryptor``
    (Plan 056 / §D-flip13-reencrypt) — the shared rule behind both durable
    repositories' ``reencrypt_legacy_plaintext()``.

    An entry that already decrypts cleanly is left alone; only a decrypt
    **failure** (non-hex, or the encryptor rejects the token) marks it as
    legacy plaintext. A real ciphertext cannot be misread as plaintext — that
    would need the encryptor to reject its own token.

    DESIGN: under ``legacy_hex=None`` an ambiguous entry refuses, never guesses
        ✅ A guess is silent and destructive: ``"41424344"`` read as hex is
           rewritten as the secret ``"ABCD"``, and a wrong-key Fernet token
           (hex of printable base64url) is re-encrypted as the "secret" —
           the next delivery is signed with garbage and nothing says why.
        ✅ An unambiguous entry (``whsec_…``, anything not valid hex of
           printable text) has one reading and still upgrades with no flag,
           so the common 3.x case needs no decision.
        ❌ A ``PlaintextFieldEncryptor`` → Fernet switch always needs
           ``legacy_hex=True`` (every such row is hex of printable text).
           Accepted — the operator knows which encryptor wrote the rows; the
           library cannot.
        Rejected — keep the printable-hex heuristic as the default and
        document "prefer ``legacy_hex=False``": the documented trap stays a
        trap for whoever skips the docs.

    Args:
        stored:     The stored ``active_secrets`` entry.
        encryptor:  The repository's (new) ``FieldEncryptor``.
        legacy_hex: How to read a legacy entry. ``None`` (default) — only an
                    unambiguous entry is upgraded (read verbatim); an
                    ambiguous one raises. ``True`` — every legacy entry is hex
                    of UTF-8 (``PlaintextFieldEncryptor`` rows). ``False`` —
                    every legacy entry is the raw 3.x secret.

    Returns:
        The new stored value, or ``None`` when the entry is already ciphertext.

    Raises:
        AmbiguousLegacySecretError: ``legacy_hex=None`` and the entry fails to
            decrypt and is valid hex of printable UTF-8.
        ValueError: ``legacy_hex=True`` and an entry is not hex of UTF-8.
        EncryptionError: ``encryptor`` fails to encrypt.

    Edge cases:
        - ⚠️ "Decrypt failed" is the only plaintext signal, so an encryptor
          that cannot read a *real* ciphertext — the wrong key, or a
          ``MultiKeyEncryptorRegistry`` whose ``kid`` was destroyed
          (``KeyDestroyedError`` is an ``EncryptionError``) — makes that
          ciphertext look like legacy plaintext. Under ``legacy_hex=None``
          it refuses (the stored hex of a Fernet token is ambiguous); under
          an explicit ``True``/``False`` it is re-encrypted as the "secret"
          (recoverable only by decrypting twice, by hand). Verify the
          encryptor reads a known-good row before passing either.
    """
    from varco_core.encryption import EncryptionError  # noqa: PLC0415 — import budget

    try:
        encryptor.decrypt(bytes.fromhex(stored)).decode("utf-8")
    except (ValueError, EncryptionError):
        pass  # not ciphertext under this encryptor — legacy plaintext
    else:
        return None
    secret = _legacy_plaintext(stored, legacy_hex)
    return encryptor.encrypt(secret.encode("utf-8")).hex()


def plan_legacy_reencryption(
    rows: Iterable[tuple[_IdT, Sequence[str]]],
    encryptor: FieldEncryptor,
    *,
    legacy_hex: bool | None = None,
) -> list[tuple[_IdT, list[str]]]:
    """
    Compute every rewrite ``reencrypt_legacy_plaintext()`` would make, and
    refuse the whole batch if any entry is ambiguous — **before** a single
    write (Plan 056 / §D-flip13-reencrypt, drift decision).

    DESIGN: plan-then-write, shared by both durable repositories
        ✅ "Nothing written" is a property of the batch, not of the first
           ambiguous row: a refusal half-way through a write loop would leave
           a mix of upgraded rows and the operator re-running with a flag
           that now also applies to rows already rewritten.
        ✅ One place states the rule; the SA and Beanie repositories only
           read rows and apply the plan.
        ❌ Holds every row's secrets in memory for the run. Accepted — a
           webhook subscription table is small, and this is a one-shot
           maintenance step.

    Args:
        rows:       ``(subscription_id, active_secrets)`` pairs as stored.
        encryptor:  The repository's (new) ``FieldEncryptor``.
        legacy_hex: Forwarded to ``reencrypt_legacy_secret`` for every entry.

    Returns:
        ``(subscription_id, new_active_secrets)`` for each row with at least
        one rewritten entry, in input order. Unchanged entries are kept
        as stored.

    Raises:
        AmbiguousLegacySecretError: ``legacy_hex=None`` and at least one
            entry is ambiguous — ``subscription_ids`` names every such row.
        ValueError: ``legacy_hex=True`` and an entry is not hex of UTF-8.
        EncryptionError: ``encryptor`` fails to encrypt.

    Example::

        plan = plan_legacy_reencryption([(pk, secrets) for pk, secrets in rows], encryptor)
        for pk, new_secrets in plan:
            ...  # write new_secrets for pk
    """
    plan: list[tuple[_IdT, list[str]]] = []
    ambiguous: list[_IdT] = []
    for subscription_id, secrets in rows:
        new_secrets: list[str] = []
        changed = False
        try:
            for entry in secrets:
                upgraded = reencrypt_legacy_secret(entry, encryptor, legacy_hex=legacy_hex)
                changed = changed or upgraded is not None
                new_secrets.append(entry if upgraded is None else upgraded)
        except AmbiguousLegacySecretError:
            # Keep scanning: the refusal names every ambiguous row at once.
            ambiguous.append(subscription_id)
            continue
        if changed:
            plan.append((subscription_id, new_secrets))
    if ambiguous:
        raise AmbiguousLegacySecretError(ambiguous)
    return plan


class WebhookSubscriptionRepository(abc.ABC):
    """
    Storage contract for ``WebhookSubscription`` (§D-D4-entity).

    Implementations MUST scope ``find_by_tenant`` strictly — a subscription
    belonging to tenant A must never be returned for tenant B (guarded by
    the conformance-style tests in every backend's integration suite and by
    the admin surface's cross-tenant test).

    Async safety: ✅ All methods are ``async def``.
    """

    @abc.abstractmethod
    async def save(self, subscription: WebhookSubscription) -> WebhookSubscription:
        """
        Insert (``pk is None``) or update (``pk`` set) ``subscription``.

        Args:
            subscription: The entity to persist.

        Returns:
            The persisted entity, with ``pk`` populated on insert.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def find_by_id(self, pk: object) -> WebhookSubscription | None:
        """
        Return the subscription with primary key ``pk``, or ``None``.

        Args:
            pk: The primary key to look up.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def find_by_tenant(self, tenant_id: str) -> list[WebhookSubscription]:
        """
        Return every subscription owned by ``tenant_id``.

        Args:
            tenant_id: The owning tenant.

        Returns:
            A list, possibly empty. Never includes another tenant's rows.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def find_active_matching(
        self, event_type: str, *, tenant_id: str | None = None
    ) -> list[WebhookSubscription]:
        """
        Return every ``ACTIVE`` subscription whose ``event_patterns`` match
        ``event_type``.

        Args:
            event_type: The event type name to match (e.g. ``"order.created"``).
            tenant_id:  Optional tenant scope. ``None`` matches across all
                        tenants — used by the dispatcher, which receives
                        events already carrying their own tenant context in
                        the payload rather than at the bus level.

        Returns:
            A list of matching, ``ACTIVE`` subscriptions. ``DISABLED``
            subscriptions are never returned here.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def delete(self, pk: object) -> None:
        """
        Delete the subscription with primary key ``pk``.

        Edge cases:
            - Deleting an unknown ``pk`` is a no-op.
        """
        raise NotImplementedError


def _matches_pattern(event_type: str, pattern: str) -> bool:
    """
    Glob-style match (``"order.*"`` matches ``"order.created"``).

    Uses ``fnmatch`` semantics via a minimal manual implementation to avoid
    importing ``fnmatch`` for a single ``*`` wildcard use case — kept
    intentionally simple; a future pattern language upgrade is a
    same-signature swap.
    """
    import fnmatch

    return fnmatch.fnmatch(event_type, pattern)


class InMemoryWebhookSubscriptionRepository(WebhookSubscriptionRepository):
    """
    Single-process ``WebhookSubscriptionRepository`` backed by a plain dict.

    ⚠️ **Single-process only** — same warning as every other
    ``InMemory*`` primitive in this codebase (``InMemoryIdempotencyStore``,
    ``InMemoryRateLimiter``). Use ``SAWebhookSubscriptionRepository`` or
    ``BeanieWebhookSubscriptionRepository`` for any multi-process deployment.

    Secrets are held in memory only — never at rest — so the FLIP-13
    encryptor requirement (Plan 056) applies to the durable backends and
    this repository is never reported as
    ``posture.webhook_secrets_plaintext``. (It is not one of the six
    ``VARCO_<SEAM>_BACKEND`` seams, so ``di.in_process_backend`` does not
    report it either — choosing it for production is the single-process
    question above, not an encryption one.)

    Thread safety:  ❌ Not thread-safe across OS threads.
    Async safety:   ✅ Coroutine-safe within one event loop via a lazily
                       created ``asyncio.Lock`` (CLAUDE.md: never construct
                       a lock at module scope or in ``__init__``).
    """

    def __init__(self) -> None:
        self._rows: dict[object, WebhookSubscription] = {}
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def save(self, subscription: WebhookSubscription) -> WebhookSubscription:
        """See ``WebhookSubscriptionRepository.save()``."""
        async with self._get_lock():
            if subscription.pk is None:
                object.__setattr__(subscription, "pk", uuid4())
            object.__setattr__(subscription, "_raw_orm", object())
            self._rows[subscription.pk] = deepcopy(subscription)
            return deepcopy(subscription)

    async def find_by_id(self, pk: object) -> WebhookSubscription | None:
        """See ``WebhookSubscriptionRepository.find_by_id()``."""
        async with self._get_lock():
            row = self._rows.get(pk)
            return deepcopy(row) if row is not None else None

    async def find_by_tenant(self, tenant_id: str) -> list[WebhookSubscription]:
        """See ``WebhookSubscriptionRepository.find_by_tenant()``."""
        async with self._get_lock():
            return [deepcopy(r) for r in self._rows.values() if r.tenant_id == tenant_id]

    async def find_active_matching(
        self, event_type: str, *, tenant_id: str | None = None
    ) -> list[WebhookSubscription]:
        """See ``WebhookSubscriptionRepository.find_active_matching()``."""
        async with self._get_lock():
            results = []
            for row in self._rows.values():
                if row.status != "ACTIVE":
                    continue
                if tenant_id is not None and row.tenant_id != tenant_id:
                    continue
                if any(_matches_pattern(event_type, p) for p in row.event_patterns):
                    results.append(deepcopy(row))
            return results

    async def delete(self, pk: object) -> None:
        """See ``WebhookSubscriptionRepository.delete()``."""
        async with self._get_lock():
            self._rows.pop(pk, None)
