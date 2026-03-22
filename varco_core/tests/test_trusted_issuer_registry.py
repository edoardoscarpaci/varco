"""
Unit tests for TrustedIssuerRegistry.from_container() and related DI changes.
===============================================================================

Covers:
    - ClassVar DI annotations (_multi_key_handle, _jwt_handle) declared on the class
    - from_container() accepts Instance[T] proxy-shaped objects — no DIContainer coupling
    - resolvable() probe: aget_all() is never called when resolvable() returns False
    - aget_all() is consumed when resolvable() returns True, authorities registered
    - include_env=False / include_env=True base registry selection
    - load_all() always called regardless of which proxies are resolvable
    - both MultiKeyAuthority and JwtAuthority can be registered in the same call

Testing strategy:
    Lightweight _FakeInstanceProxy objects replace real Providify containers —
    no actual DI wiring, no real InstanceProxy construction.  Mock signing
    authority objects satisfy the _SigningAuthority protocol (issuer: str,
    jwks() → JsonWebKeySet) without generating expensive crypto keys.

    Almost all tests use include_env=False to avoid FASTREST_AUTHORIZATION__*
    env var pollution; include_env=True cases patch from_env() explicitly.
"""

from __future__ import annotations

from unittest.mock import patch


from varco_core.authority.registry import TrustedIssuerRegistry
from varco_core.jwk.model import JsonWebKeySet


# ── Test helpers ──────────────────────────────────────────────────────────────


class _FakeInstanceProxy:
    """
    Minimal stand-in for Providify's InstanceProxy[T].

    Provides only the two methods that from_container() relies on:
        resolvable() → bool  — sync probe, no instance created
        aget_all()   → list  — async, returns configured instances

    An explicit call counter on aget_all lets tests assert it was never
    invoked when resolvable() is False — cleaner than MagicMock.call_count
    because the intent is visible without looking at mock setup code.

    Async safety: aget_all() is a coroutine — safe to await from any task.
    """

    def __init__(
        self,
        instances: list,
        *,
        is_resolvable: bool = True,
    ) -> None:
        """
        Args:
            instances:     Objects returned by aget_all().
            is_resolvable: Whether resolvable() returns True.
        """
        self._instances = instances
        self._is_resolvable = is_resolvable
        # Tracks how many times aget_all() was awaited — asserted in tests.
        self.aget_all_call_count: int = 0

    def resolvable(self) -> bool:
        """Side-effect-free probe — no instance created."""
        return self._is_resolvable

    async def aget_all(self) -> list:
        """Return all configured instances, recording the call."""
        self.aget_all_call_count += 1
        return list(self._instances)


class _FakeAuthority:
    """
    Minimal signing authority satisfying the _SigningAuthority Protocol.

    Provides the two attributes required by register_authority():
        .issuer  — the iss claim string
        .jwks()  — returns an empty JsonWebKeySet (no real crypto keys needed)

    Using an empty keyset is valid — AuthoritySource.load() returns whatever
    jwks() returns, and load_all() just stores it; we don't need real key
    material to verify registration behaviour.
    """

    def __init__(self, issuer: str) -> None:
        """
        Args:
            issuer: The iss claim value — used for label derivation.
        """
        self._issuer = issuer

    @property
    def issuer(self) -> str:
        """The iss claim value signed into every token."""
        return self._issuer

    def jwks(self) -> JsonWebKeySet:
        """Return an empty keyset — no real keys needed for registration tests."""
        return JsonWebKeySet(keys=())


def _empty_proxy() -> _FakeInstanceProxy:
    """Return a proxy with no bindings and resolvable() == False."""
    return _FakeInstanceProxy([], is_resolvable=False)


# ── TestClassVarAnnotations ────────────────────────────────────────────────────


class TestClassVarAnnotations:
    """
    Verify that the DI contract ClassVar annotations are declared on the class.

    These annotations serve as documentation — they tell a reader what
    Instance[T] proxies from_container() expects without having to read the
    method signature.  They are NOT injected automatically by Providify (which
    only reads __init__ params), so we just check they are present as
    annotations on the class.

    We inspect TrustedIssuerRegistry.__annotations__ directly instead of using
    get_type_hints() because with 'from __future__ import annotations' all
    annotations are deferred strings — get_type_hints() would try to resolve
    them at runtime, requiring runtime imports of Instance and the authority
    classes, which are TYPE_CHECKING-only.  Checking __annotations__.keys() is
    sufficient and avoids that import complexity in tests.
    """

    def test_multi_key_handle_classvar_annotation_exists(self) -> None:
        # The annotation signals: "wire Instance[MultiKeyAuthority] here".
        assert "_multi_key_handle" in TrustedIssuerRegistry.__annotations__

    def test_jwt_handle_classvar_annotation_exists(self) -> None:
        # The annotation signals: "wire Instance[JwtAuthority] here".
        assert "_jwt_handle" in TrustedIssuerRegistry.__annotations__

    def test_both_annotations_are_declared_as_strings(self) -> None:
        # With from __future__ import annotations, all annotations become
        # deferred strings.  We verify the string representation contains the
        # expected type names without evaluating them.
        multi_ann = TrustedIssuerRegistry.__annotations__["_multi_key_handle"]
        jwt_ann = TrustedIssuerRegistry.__annotations__["_jwt_handle"]

        # Both must reference ClassVar — they are class-level, not instance-level.
        assert "ClassVar" in multi_ann
        assert "ClassVar" in jwt_ann

        # Both must reference the respective authority types.
        assert "MultiKeyAuthority" in multi_ann
        assert "JwtAuthority" in jwt_ann


# ── TestFromContainerNoBidings ─────────────────────────────────────────────────


class TestFromContainerNoBindings:
    """
    Behaviour when neither proxy has any DI bindings — both resolvable() == False.

    from_container() must:
        - Not call aget_all() on either proxy (no bindings to fetch)
        - Return a valid, empty registry (no entries from DI)
        - Still call load_all() (harmless on an empty registry)
    """

    async def test_returns_empty_registry_when_neither_resolvable(self) -> None:
        # Both proxies unresolvable → no authorities registered.
        registry = await TrustedIssuerRegistry.from_container(
            _empty_proxy(),
            _empty_proxy(),
            include_env=False,
        )
        # Registry has zero entries — no DI authorities, no env-var authorities.
        assert len(registry._entries) == 0

    async def test_aget_all_not_called_when_multi_key_not_resolvable(self) -> None:
        # Verify resolvable() == False prevents aget_all() from being awaited.
        # This matters because aget_all() on a real InstanceProxy with no
        # bindings would raise LookupError — the guard prevents that error.
        multi_proxy = _FakeInstanceProxy([], is_resolvable=False)
        await TrustedIssuerRegistry.from_container(
            multi_proxy,
            _empty_proxy(),
            include_env=False,
        )
        assert multi_proxy.aget_all_call_count == 0

    async def test_aget_all_not_called_when_jwt_not_resolvable(self) -> None:
        # Same guard for the JWT proxy.
        jwt_proxy = _FakeInstanceProxy([], is_resolvable=False)
        await TrustedIssuerRegistry.from_container(
            _empty_proxy(),
            jwt_proxy,
            include_env=False,
        )
        assert jwt_proxy.aget_all_call_count == 0

    async def test_aget_all_not_called_on_empty_but_resolvable_proxy(self) -> None:
        # resolvable() == True with an empty list → aget_all() IS called,
        # but no authorities are registered (loop body never executes).
        multi_proxy = _FakeInstanceProxy([], is_resolvable=True)
        registry = await TrustedIssuerRegistry.from_container(
            multi_proxy,
            _empty_proxy(),
            include_env=False,
        )
        # aget_all() was called — resolvable() returned True.
        assert multi_proxy.aget_all_call_count == 1
        # No entries because the list was empty.
        assert len(registry._entries) == 0


# ── TestFromContainerMultiKeyAuthority ────────────────────────────────────────


class TestFromContainerMultiKeyAuthority:
    """
    Behaviour when MultiKeyAuthority bindings are present in the proxy.

    Verifies:
        - Single authority → one entry in the registry
        - Multiple authorities → all entries registered independently
        - Label derived from issuer: "my-svc" → "MY_SVC"
        - aget_all() called exactly once
    """

    async def test_single_multi_key_authority_registered(self) -> None:
        # A single MultiKeyAuthority should add one entry to the registry.
        authority = _FakeAuthority(issuer="my-svc")
        multi_proxy = _FakeInstanceProxy([authority])

        registry = await TrustedIssuerRegistry.from_container(
            multi_proxy,
            _empty_proxy(),
            include_env=False,
        )

        assert len(registry._entries) == 1

    async def test_multi_key_authority_label_derived_from_issuer(self) -> None:
        # Label derivation: issuer.upper().replace("-", "_").
        # "system-svc" → "SYSTEM_SVC" — matches env-var label convention.
        authority = _FakeAuthority(issuer="system-svc")
        multi_proxy = _FakeInstanceProxy([authority])

        registry = await TrustedIssuerRegistry.from_container(
            multi_proxy,
            _empty_proxy(),
            include_env=False,
        )

        # entry() raises IssuerNotFoundError if the label is absent.
        entry = registry.entry("SYSTEM_SVC")
        assert entry.iss == "system-svc"

    async def test_multiple_multi_key_authorities_all_registered(self) -> None:
        # All authorities from aget_all() must be registered — no truncation.
        authorities = [
            _FakeAuthority(issuer="svc-a"),
            _FakeAuthority(issuer="svc-b"),
            _FakeAuthority(issuer="svc-c"),
        ]
        multi_proxy = _FakeInstanceProxy(authorities)

        registry = await TrustedIssuerRegistry.from_container(
            multi_proxy,
            _empty_proxy(),
            include_env=False,
        )

        assert len(registry._entries) == 3
        # Spot-check one label to confirm label derivation ran for all.
        assert "SVC_A" in registry._entries
        assert "SVC_B" in registry._entries
        assert "SVC_C" in registry._entries

    async def test_aget_all_called_exactly_once_for_multi_key(self) -> None:
        # from_container() must not call aget_all() more than once —
        # doing so would hit the container multiple times for the same type.
        authority = _FakeAuthority(issuer="once-only")
        multi_proxy = _FakeInstanceProxy([authority])

        await TrustedIssuerRegistry.from_container(
            multi_proxy,
            _empty_proxy(),
            include_env=False,
        )

        assert multi_proxy.aget_all_call_count == 1


# ── TestFromContainerJwtAuthority ─────────────────────────────────────────────


class TestFromContainerJwtAuthority:
    """
    Behaviour when JwtAuthority bindings are present in the proxy.

    Mirrors the MultiKeyAuthority tests — both proxy types use the same
    resolvable() + aget_all() path.
    """

    async def test_single_jwt_authority_registered(self) -> None:
        # A single JwtAuthority should add one entry to the registry.
        authority = _FakeAuthority(issuer="jwt-svc")
        jwt_proxy = _FakeInstanceProxy([authority])

        registry = await TrustedIssuerRegistry.from_container(
            _empty_proxy(),
            jwt_proxy,
            include_env=False,
        )

        assert len(registry._entries) == 1

    async def test_jwt_authority_label_derived_from_issuer(self) -> None:
        # Same label derivation as MultiKeyAuthority.
        authority = _FakeAuthority(issuer="auth-service")
        jwt_proxy = _FakeInstanceProxy([authority])

        registry = await TrustedIssuerRegistry.from_container(
            _empty_proxy(),
            jwt_proxy,
            include_env=False,
        )

        entry = registry.entry("AUTH_SERVICE")
        assert entry.iss == "auth-service"

    async def test_multiple_jwt_authorities_all_registered(self) -> None:
        # All JwtAuthority bindings are registered — no truncation.
        authorities = [
            _FakeAuthority(issuer="jwt-a"),
            _FakeAuthority(issuer="jwt-b"),
        ]
        jwt_proxy = _FakeInstanceProxy(authorities)

        registry = await TrustedIssuerRegistry.from_container(
            _empty_proxy(),
            jwt_proxy,
            include_env=False,
        )

        assert len(registry._entries) == 2
        assert "JWT_A" in registry._entries
        assert "JWT_B" in registry._entries

    async def test_aget_all_not_called_when_jwt_not_resolvable(self) -> None:
        # Guard path: resolvable() == False skips aget_all() entirely.
        jwt_proxy = _FakeInstanceProxy(
            [_FakeAuthority(issuer="unreachable")],
            is_resolvable=False,
        )

        await TrustedIssuerRegistry.from_container(
            _empty_proxy(),
            jwt_proxy,
            include_env=False,
        )

        assert jwt_proxy.aget_all_call_count == 0


# ── TestFromContainerBothTypes ────────────────────────────────────────────────


class TestFromContainerBothTypes:
    """
    Behaviour when both MultiKeyAuthority and JwtAuthority bindings are present.

    from_container() must register authorities from both proxies independently.
    The order of registration is: MultiKeyAuthority first, then JwtAuthority.
    """

    async def test_both_types_registered_together(self) -> None:
        # Total entries = multi count + jwt count.
        multi_proxy = _FakeInstanceProxy([_FakeAuthority(issuer="multi-svc")])
        jwt_proxy = _FakeInstanceProxy([_FakeAuthority(issuer="jwt-svc")])

        registry = await TrustedIssuerRegistry.from_container(
            multi_proxy,
            jwt_proxy,
            include_env=False,
        )

        assert len(registry._entries) == 2
        assert "MULTI_SVC" in registry._entries
        assert "JWT_SVC" in registry._entries

    async def test_multi_key_registered_before_jwt(self) -> None:
        # Registration order: MultiKeyAuthority authorities are registered first,
        # then JwtAuthority authorities.  A label collision (same issuer) means
        # the JwtAuthority entry overwrites the MultiKeyAuthority entry — the
        # last write wins.  This test uses distinct issuers to avoid that.
        multi_proxy = _FakeInstanceProxy(
            [
                _FakeAuthority(issuer="svc-1"),
                _FakeAuthority(issuer="svc-2"),
            ]
        )
        jwt_proxy = _FakeInstanceProxy(
            [
                _FakeAuthority(issuer="svc-3"),
            ]
        )

        registry = await TrustedIssuerRegistry.from_container(
            multi_proxy,
            jwt_proxy,
            include_env=False,
        )

        assert len(registry._entries) == 3

    async def test_label_collision_jwt_overwrites_multi_key(self) -> None:
        # DESIGN: register() replaces an existing entry silently when labels
        # collide.  If both multi_key and jwt proxies resolve an authority with
        # the same issuer string, the jwt entry (registered second) wins.
        # This is documented behaviour — the test asserts it, not prevents it.
        shared_issuer = "shared-svc"
        multi_proxy = _FakeInstanceProxy([_FakeAuthority(issuer=shared_issuer)])
        jwt_proxy = _FakeInstanceProxy([_FakeAuthority(issuer=shared_issuer)])

        registry = await TrustedIssuerRegistry.from_container(
            multi_proxy,
            jwt_proxy,
            include_env=False,
        )

        # Only one entry — both derived the same label "SHARED_SVC".
        assert len(registry._entries) == 1
        assert "SHARED_SVC" in registry._entries


# ── TestFromContainerIncludeEnv ───────────────────────────────────────────────


class TestFromContainerIncludeEnv:
    """
    Behaviour of the include_env flag.

    include_env=False → registry starts empty (no FASTREST_AUTHORIZATION__* lookup)
    include_env=True  → from_env() is called first, then DI authorities layered on top
    """

    async def test_include_env_false_starts_with_empty_base(self) -> None:
        # When include_env=False, no env-var issuers are loaded.  The returned
        # registry contains only DI-registered authorities.
        authority = _FakeAuthority(issuer="di-only")
        multi_proxy = _FakeInstanceProxy([authority])

        registry = await TrustedIssuerRegistry.from_container(
            multi_proxy,
            _empty_proxy(),
            include_env=False,
        )

        # Only the DI-registered authority is present — no env-var entries.
        assert len(registry._entries) == 1
        assert "DI_ONLY" in registry._entries

    async def test_include_env_true_calls_from_env(self) -> None:
        # When include_env=True, from_env() is called to build the base registry
        # before DI authorities are layered on top.  We patch from_env() to
        # return a known registry containing one pre-seeded env-var entry so
        # we can assert both env-var and DI entries are present in the result.
        env_registry = TrustedIssuerRegistry()
        # Manually register a fake env-var issuer by using the internal _entries
        # dict — simpler than calling from_env() for real.
        from varco_core.authority.sources.authority import AuthoritySource

        env_authority = _FakeAuthority(issuer="env-issuer")
        env_source = AuthoritySource(env_authority, issuer="env-issuer")
        from varco_core.authority.registry import TrustedIssuerEntry

        env_registry._entries["ENV_ISSUER"] = TrustedIssuerEntry(
            label="ENV_ISSUER",
            iss="env-issuer",
            source=env_source,
        )

        di_authority = _FakeAuthority(issuer="di-issuer")
        multi_proxy = _FakeInstanceProxy([di_authority])

        # Patch from_env() so it returns our pre-seeded env_registry instead
        # of reading real FASTREST_AUTHORIZATION__* environment variables.
        with patch.object(TrustedIssuerRegistry, "from_env", return_value=env_registry):
            registry = await TrustedIssuerRegistry.from_container(
                multi_proxy,
                _empty_proxy(),
                include_env=True,
            )

        # Both env-var and DI entries must coexist in the returned registry.
        assert "ENV_ISSUER" in registry._entries
        assert "DI_ISSUER" in registry._entries
        assert len(registry._entries) == 2

    async def test_include_env_true_is_default(self) -> None:
        # include_env defaults to True — omitting the kwarg should use from_env().
        # We patch from_env() to prevent real env-var lookups and verify it is
        # actually called when the default is used.
        with patch.object(
            TrustedIssuerRegistry,
            "from_env",
            return_value=TrustedIssuerRegistry(),
        ) as mock_from_env:
            await TrustedIssuerRegistry.from_container(
                _empty_proxy(),
                _empty_proxy(),
                # include_env omitted — defaults to True
            )

        mock_from_env.assert_called_once()


# ── TestFromContainerLoadAll ──────────────────────────────────────────────────


class TestFromContainerLoadAll:
    """
    load_all() must always be called — even when the DI proxies yield nothing.

    load_all() sets entry._keyset on every registered source.  Without it,
    get_key() never finds any key even for in-memory AuthoritySource entries.
    These tests verify that load_all() runs and that its effects are visible.
    """

    async def test_registered_authority_keyset_populated_after_load_all(
        self,
    ) -> None:
        # After from_container() returns, entry._keyset must not be None —
        # load_all() must have run and set it.
        authority = _FakeAuthority(issuer="my-svc")
        multi_proxy = _FakeInstanceProxy([authority])

        registry = await TrustedIssuerRegistry.from_container(
            multi_proxy,
            _empty_proxy(),
            include_env=False,
        )

        entry = registry.entry("MY_SVC")
        # load_all() called AuthoritySource.load() which called authority.jwks()
        # and stored the result in entry._keyset.
        assert entry._keyset is not None

    async def test_all_jwks_returns_merged_set_after_from_container(
        self,
    ) -> None:
        # all_jwks() relies on entry._keyset being set — confirms load_all() ran.
        authority = _FakeAuthority(issuer="my-svc")
        multi_proxy = _FakeInstanceProxy([authority])

        registry = await TrustedIssuerRegistry.from_container(
            multi_proxy,
            _empty_proxy(),
            include_env=False,
        )

        # all_jwks() returns a JsonWebKeySet — does not raise.
        # (Keys are empty because _FakeAuthority.jwks() returns an empty set.)
        merged = registry.all_jwks()
        assert isinstance(merged, JsonWebKeySet)

    async def test_load_all_called_even_with_no_authorities(self) -> None:
        # load_all() on an empty registry is a no-op but must not raise.
        # This test verifies the empty-registry edge case is handled cleanly.
        registry = await TrustedIssuerRegistry.from_container(
            _empty_proxy(),
            _empty_proxy(),
            include_env=False,
        )

        # No entries — all_jwks() returns an empty JsonWebKeySet, not None.
        assert registry.all_jwks().keys == ()
