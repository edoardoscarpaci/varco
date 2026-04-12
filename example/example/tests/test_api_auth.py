"""
tests.test_api_auth
===================
Integration tests for the authentication and RBAC system of the Post API.

Coverage
--------
┌─────────────────────────────────┬───────────────────────────────────────────┐
│ Section                         │ What is tested                            │
├─────────────────────────────────┼───────────────────────────────────────────┤
│ Login (POST /auth/login)        │ success (alice, bob), bad credentials 401 │
│ Identity (GET /auth/me)         │ anonymous, alice, bob, bad token           │
│ Anonymous access                │ read-only OK, mutating ops → 403           │
│ Editor access (bob)             │ create own post, read any, patch/delete    │
│                                 │ own → 200/204, other's → 403               │
│ Admin access (alice)            │ full CRUD on any post                      │
│ Summary endpoint auth           │ caller field reflects JWT sub              │
└─────────────────────────────────┴───────────────────────────────────────────┘

Authorization policy (see example/example/authorizer.py for full table):

    Role       │ list │ read │ create │ update │ delete
    ───────────┼──────┼──────┼────────┼────────┼───────
    admin      │  ✅  │  ✅  │  ✅    │  ✅    │  ✅    (any post)
    editor     │  ✅  │  ✅  │  ✅    │  own ✅│  own ✅
    anonymous  │  ✅  │  ✅  │  ❌    │  ❌    │  ❌

Strategy
--------
- All tests use a real running server (Docker + uvicorn) via the ``client``
  fixture from ``conftest.py``.
- ``alice_token`` and ``bob_token`` are session-scoped — obtained once via
  ``POST /auth/login`` and reused across tests.
- Each test that creates data uses a unique title so assertions are data-level
  isolated without a DB reset between tests.

DESIGN: separate test file for auth vs. CRUD
    ✅ Auth and RBAC paths are exercised independently of CRUD correctness.
    ✅ A failing auth test doesn't obscure a CRUD failure (or vice versa).
    ❌ Both files start from the same ``running_server`` — small coupling.

Thread safety:  ⚠️ Tests share one server — do not run with pytest-xdist.
Async safety:   ✅ All tests are ``async def``; asyncio_mode = auto.

📚 Docs
- 🔍 https://www.rfc-editor.org/rfc/rfc6750 — Bearer token usage (Authorization header)
- 🐍 https://www.python-httpx.org/async/ — httpx.AsyncClient
"""

from __future__ import annotations

import httpx
import pytest


# ── Login tests ────────────────────────────────────────────────────────────────


@pytest.mark.integration
async def test_login_alice_returns_token(client: httpx.AsyncClient) -> None:
    """
    ``POST /auth/login`` with alice's credentials returns a 200 with a
    well-formed ``TokenResponse``.

    Exercises:
    - AuthRouter.login() credential validation path.
    - JwtAuthority.sign() — RS256 JWT construction.

    Edge cases:
        - ``access_token`` is a non-empty string starting with ``eyJ`` (JWT header).
        - ``token_type`` is always ``"bearer"`` (RFC 6750).
        - ``expires_in`` is 3600 (1-hour default in ``example.auth``).
    """
    resp = await client.post(
        "/auth/login",
        json={"username": "alice", "password": "alice123"},
    )
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert "access_token" in body, "access_token missing from response"
    assert body["token_type"] == "bearer"
    assert body["expires_in"] == 3600

    # JWT tokens start with "eyJ" (base64url-encoded {"alg":...} header)
    assert body["access_token"].startswith(
        "eyJ"
    ), "access_token does not look like a JWT"


@pytest.mark.integration
async def test_login_bob_returns_token(client: httpx.AsyncClient) -> None:
    """
    ``POST /auth/login`` with bob's credentials also returns 200.

    Verifies both demo users are registered and can obtain tokens.
    """
    resp = await client.post(
        "/auth/login",
        json={"username": "bob", "password": "bob123"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["access_token"].startswith("eyJ")


@pytest.mark.integration
async def test_login_wrong_password_returns_401(client: httpx.AsyncClient) -> None:
    """
    ``POST /auth/login`` with a correct username but wrong password returns 401.

    Exercises the ``DEMO_USERS`` lookup and password comparison path.

    Edge cases:
        - Response body includes ``detail`` explaining how to obtain credentials.
        - ``WWW-Authenticate: Bearer`` header is required by RFC 6750 for 401 responses.
    """
    resp = await client.post(
        "/auth/login",
        json={"username": "alice", "password": "wrongpassword"},
    )
    assert resp.status_code == 401, resp.text
    # FastAPI error middleware returns a structured JSON body.
    assert "detail" in resp.json()


@pytest.mark.integration
async def test_login_unknown_user_returns_401(client: httpx.AsyncClient) -> None:
    """
    ``POST /auth/login`` with a username not in ``DEMO_USERS`` returns 401.

    Edge cases:
        - Usernames are case-sensitive: ``"Alice"`` is not ``"alice"``.
    """
    resp = await client.post(
        "/auth/login",
        json={"username": "nonexistent", "password": "whatever"},
    )
    assert resp.status_code == 401, resp.text


# ── /auth/me tests ─────────────────────────────────────────────────────────────


@pytest.mark.integration
async def test_me_anonymous_when_no_token(client: httpx.AsyncClient) -> None:
    """
    ``GET /auth/me`` without a Bearer token returns ``anonymous=true``.

    Exercises:
    - ``RequestContextMiddleware`` sets an anonymous ``AuthContext`` when no
      valid token is present.
    - ``get_auth_context_or_none()`` returns ``None`` → ``anonymous=True``.

    Edge cases:
        - The endpoint is always reachable (no auth required) so it doubles as
          a liveness check: if this returns 5xx the server is unhealthy.
    """
    resp = await client.get("/auth/me")
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["anonymous"] is True
    assert body["user_id"] is None
    assert body["roles"] == []


@pytest.mark.integration
async def test_me_returns_alice_identity(
    client: httpx.AsyncClient,
    alice_token: str,
) -> None:
    """
    ``GET /auth/me`` with alice's token returns her user_id and roles.

    Exercises:
    - ``JwtBearerAuth.verify()`` decodes and validates the RS256 token.
    - ``TrustedIssuerRegistry`` matches the ``iss`` claim to the registered authority.
    - ``RequestContextMiddleware`` stores the decoded ``AuthContext`` in the ContextVar.
    - ``me()`` handler reads the ContextVar and returns ``MeResponse``.

    Edge cases:
        - ``user_id`` matches ``DEMO_USERS["alice"]["user_id"]`` exactly.
        - ``roles`` is sorted alphabetically (``me()`` calls ``sorted(ctx.roles)``).
    """
    resp = await client.get(
        "/auth/me",
        headers={"Authorization": f"Bearer {alice_token}"},
    )
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["anonymous"] is False
    assert body["user_id"] == "00000000-0000-0000-0000-000000000001"
    assert "admin" in body["roles"]


@pytest.mark.integration
async def test_me_returns_bob_identity(
    client: httpx.AsyncClient,
    bob_token: str,
) -> None:
    """
    ``GET /auth/me`` with bob's token returns his user_id and ``editor`` role.
    """
    resp = await client.get(
        "/auth/me",
        headers={"Authorization": f"Bearer {bob_token}"},
    )
    assert resp.status_code == 200, resp.text

    body = resp.json()
    assert body["anonymous"] is False
    assert body["user_id"] == "00000000-0000-0000-0000-000000000002"
    assert "editor" in body["roles"]


@pytest.mark.integration
async def test_me_with_garbage_token_is_anonymous(client: httpx.AsyncClient) -> None:
    """
    ``GET /auth/me`` with a malformed Bearer token returns anonymous.

    ``JwtBearerAuth`` is configured with ``required=False`` in ``create_app()``
    so a verification failure falls back to an anonymous context rather than
    returning 401.

    Edge cases:
        - A valid JWT from a different issuer (not in TrustedIssuerRegistry)
          would also be treated as anonymous, not 401.
    """
    resp = await client.get(
        "/auth/me",
        headers={"Authorization": "Bearer this-is-not-a-jwt"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["anonymous"] is True


# ── Anonymous access tests ────────────────────────────────────────────────────


@pytest.mark.integration
async def test_anonymous_can_list_posts(client: httpx.AsyncClient) -> None:
    """
    ``GET /v1/posts`` without authentication returns 200.

    Anonymous users have public read-only access (see PostAuthorizer).
    """
    resp = await client.get("/v1/posts")
    assert resp.status_code == 200, resp.text
    assert "results" in resp.json()


@pytest.mark.integration
async def test_anonymous_cannot_create_post_returns_403(
    client: httpx.AsyncClient,
) -> None:
    """
    ``POST /v1/posts`` without authentication returns 403.

    Exercises:
    - PostAuthorizer rejects anonymous CREATE.
    - ErrorMiddleware maps ``ServiceAuthorizationError`` → 403.
    """
    resp = await client.post(
        "/v1/posts",
        json={"title": "Anon create attempt", "body": "Should fail"},
    )
    assert (
        resp.status_code == 403
    ), f"Expected 403 for anonymous create, got {resp.status_code}: {resp.text}"


@pytest.mark.integration
async def test_anonymous_cannot_patch_post_returns_403(
    client: httpx.AsyncClient,
    alice_token: str,
) -> None:
    """
    ``PATCH /v1/posts/{id}`` without authentication returns 403.

    Setup: alice creates a post (authenticated).
    Test:  anonymous PATCH attempt → 403.

    Edge cases:
        - The 403 is from PostAuthorizer before the DB UPDATE is issued —
          no data is modified.
    """
    # Alice creates the post to patch.
    create_resp = await client.post(
        "/v1/posts",
        json={"title": "Anon patch target", "body": "Unchanged"},
        headers={"Authorization": f"Bearer {alice_token}"},
    )
    assert create_resp.status_code == 201
    pk = create_resp.json()["pk"]

    # Anonymous PATCH attempt.
    patch_resp = await client.patch(
        f"/v1/posts/{pk}",
        json={"title": "Should not change"},
    )
    assert (
        patch_resp.status_code == 403
    ), f"Expected 403 for anonymous patch, got {patch_resp.status_code}: {patch_resp.text}"


@pytest.mark.integration
async def test_anonymous_cannot_delete_post_returns_403(
    client: httpx.AsyncClient,
    alice_token: str,
) -> None:
    """
    ``DELETE /v1/posts/{id}`` without authentication returns 403.
    """
    create_resp = await client.post(
        "/v1/posts",
        json={"title": "Anon delete target", "body": "Survives"},
        headers={"Authorization": f"Bearer {alice_token}"},
    )
    assert create_resp.status_code == 201
    pk = create_resp.json()["pk"]

    delete_resp = await client.delete(f"/v1/posts/{pk}")
    assert (
        delete_resp.status_code == 403
    ), f"Expected 403 for anonymous delete, got {delete_resp.status_code}: {delete_resp.text}"

    # Verify the post still exists — the 403 blocked the delete.
    get_resp = await client.get(f"/v1/posts/{pk}")
    assert get_resp.status_code == 200, "Post was deleted despite 403"


# ── Admin (alice) access tests ────────────────────────────────────────────────


@pytest.mark.integration
async def test_admin_can_create_post(
    client: httpx.AsyncClient,
    alice_token: str,
) -> None:
    """
    Alice (admin) can create a post and the ``author_id`` is set to her user_id.

    Exercises:
    - ``PostService._prepare_for_create()`` stamps ``author_id`` from the JWT sub.
    - The JWT sub matches ``DEMO_USERS["alice"]["user_id"]``.
    """
    resp = await client.post(
        "/v1/posts",
        json={"title": "Alice admin post", "body": "Admin creates freely"},
        headers={"Authorization": f"Bearer {alice_token}"},
    )
    assert resp.status_code == 201, resp.text

    body = resp.json()
    assert body["title"] == "Alice admin post"
    # author_id must match alice's stable user_id from DEMO_USERS.
    assert body["author_id"] == "00000000-0000-0000-0000-000000000001"


@pytest.mark.integration
async def test_admin_can_patch_any_post(
    client: httpx.AsyncClient,
    alice_token: str,
    bob_token: str,
) -> None:
    """
    Alice (admin) can PATCH a post that was created by bob.

    Verifies that admin bypasses the ownership check in ``PostAuthorizer``.
    """
    # Bob creates a post.
    create_resp = await client.post(
        "/v1/posts",
        json={"title": "Bob's post for admin patch", "body": "Bob wrote this"},
        headers={"Authorization": f"Bearer {bob_token}"},
    )
    assert create_resp.status_code == 201
    pk = create_resp.json()["pk"]

    # Alice patches bob's post.
    patch_resp = await client.patch(
        f"/v1/posts/{pk}",
        json={"title": "Admin patched this"},
        headers={"Authorization": f"Bearer {alice_token}"},
    )
    assert (
        patch_resp.status_code == 200
    ), f"Admin PATCH of other's post failed: {patch_resp.status_code} {patch_resp.text}"
    assert patch_resp.json()["title"] == "Admin patched this"


@pytest.mark.integration
async def test_admin_can_delete_any_post(
    client: httpx.AsyncClient,
    alice_token: str,
    bob_token: str,
) -> None:
    """
    Alice (admin) can DELETE a post originally created by bob.

    Verifies admin bypass of the ownership check for DELETE.
    """
    # Bob creates the post.
    create_resp = await client.post(
        "/v1/posts",
        json={"title": "Bob's post for admin delete", "body": "Doomed"},
        headers={"Authorization": f"Bearer {bob_token}"},
    )
    assert create_resp.status_code == 201
    pk = create_resp.json()["pk"]

    # Alice deletes it.
    delete_resp = await client.delete(
        f"/v1/posts/{pk}",
        headers={"Authorization": f"Bearer {alice_token}"},
    )
    assert (
        delete_resp.status_code == 204
    ), f"Admin DELETE of other's post failed: {delete_resp.status_code} {delete_resp.text}"

    # Confirm gone.
    get_resp = await client.get(f"/v1/posts/{pk}")
    assert get_resp.status_code == 404


# ── Editor (bob) access tests ─────────────────────────────────────────────────


@pytest.mark.integration
async def test_editor_can_create_post(
    client: httpx.AsyncClient,
    bob_token: str,
) -> None:
    """
    Bob (editor) can create a post and becomes its author.

    Exercises:
    - PostAuthorizer grants CREATE to editors.
    - ``author_id`` is stamped from bob's JWT sub.
    """
    resp = await client.post(
        "/v1/posts",
        json={"title": "Bob's own post", "body": "Editor creates freely"},
        headers={"Authorization": f"Bearer {bob_token}"},
    )
    assert resp.status_code == 201, resp.text

    body = resp.json()
    assert body["title"] == "Bob's own post"
    # author_id must match bob's stable user_id.
    assert body["author_id"] == "00000000-0000-0000-0000-000000000002"


@pytest.mark.integration
async def test_editor_can_patch_own_post(
    client: httpx.AsyncClient,
    bob_token: str,
) -> None:
    """
    Bob (editor) can PATCH a post he created.

    Exercises the ownership check: ``entity.author_id == ctx.user_id``.
    """
    # Bob creates the post.
    create_resp = await client.post(
        "/v1/posts",
        json={"title": "Bob's patchable post", "body": "Original"},
        headers={"Authorization": f"Bearer {bob_token}"},
    )
    assert create_resp.status_code == 201
    pk = create_resp.json()["pk"]

    # Bob patches his own post.
    patch_resp = await client.patch(
        f"/v1/posts/{pk}",
        json={"title": "Bob patched his own post"},
        headers={"Authorization": f"Bearer {bob_token}"},
    )
    assert (
        patch_resp.status_code == 200
    ), f"Editor PATCH of own post failed: {patch_resp.status_code} {patch_resp.text}"
    assert patch_resp.json()["title"] == "Bob patched his own post"


@pytest.mark.integration
async def test_editor_can_delete_own_post(
    client: httpx.AsyncClient,
    bob_token: str,
) -> None:
    """
    Bob (editor) can DELETE a post he created.
    """
    create_resp = await client.post(
        "/v1/posts",
        json={"title": "Bob's deletable post", "body": "Temporary"},
        headers={"Authorization": f"Bearer {bob_token}"},
    )
    assert create_resp.status_code == 201
    pk = create_resp.json()["pk"]

    delete_resp = await client.delete(
        f"/v1/posts/{pk}",
        headers={"Authorization": f"Bearer {bob_token}"},
    )
    assert (
        delete_resp.status_code == 204
    ), f"Editor DELETE of own post failed: {delete_resp.status_code} {delete_resp.text}"

    # Verify deletion.
    get_resp = await client.get(f"/v1/posts/{pk}")
    assert get_resp.status_code == 404


@pytest.mark.integration
async def test_editor_cannot_patch_others_post_returns_403(
    client: httpx.AsyncClient,
    alice_token: str,
    bob_token: str,
) -> None:
    """
    Bob (editor) cannot PATCH a post created by alice.

    Exercises the ownership check rejection path in PostAuthorizer:
    ``entity.author_id (alice) != ctx.user_id (bob)`` → 403.

    Edge cases:
        - The 403 is raised AFTER alice's post is loaded (ownership check
          requires the entity).  The DB read happens but the UPDATE does not.
    """
    # Alice creates the post.
    create_resp = await client.post(
        "/v1/posts",
        json={"title": "Alice's post for editor patch test", "body": "Alice's"},
        headers={"Authorization": f"Bearer {alice_token}"},
    )
    assert create_resp.status_code == 201
    pk = create_resp.json()["pk"]

    # Bob attempts to patch alice's post.
    patch_resp = await client.patch(
        f"/v1/posts/{pk}",
        json={"title": "Bob tried to patch alice's post"},
        headers={"Authorization": f"Bearer {bob_token}"},
    )
    assert patch_resp.status_code == 403, (
        f"Expected 403 for editor patch of other's post, "
        f"got {patch_resp.status_code}: {patch_resp.text}"
    )

    # Alice's post title must be unchanged.
    get_resp = await client.get(f"/v1/posts/{pk}")
    assert get_resp.json()["title"] == "Alice's post for editor patch test"


@pytest.mark.integration
async def test_editor_cannot_delete_others_post_returns_403(
    client: httpx.AsyncClient,
    alice_token: str,
    bob_token: str,
) -> None:
    """
    Bob (editor) cannot DELETE a post created by alice.

    Exercises the same ownership check as the PATCH case.

    Edge cases:
        - The post must still exist after the 403 — no data was deleted.
    """
    create_resp = await client.post(
        "/v1/posts",
        json={"title": "Alice's post for editor delete test", "body": "Safe"},
        headers={"Authorization": f"Bearer {alice_token}"},
    )
    assert create_resp.status_code == 201
    pk = create_resp.json()["pk"]

    # Bob attempts to delete alice's post.
    delete_resp = await client.delete(
        f"/v1/posts/{pk}",
        headers={"Authorization": f"Bearer {bob_token}"},
    )
    assert delete_resp.status_code == 403, (
        f"Expected 403 for editor delete of other's post, "
        f"got {delete_resp.status_code}: {delete_resp.text}"
    )

    # Alice's post still exists.
    get_resp = await client.get(f"/v1/posts/{pk}")
    assert get_resp.status_code == 200, "Post was deleted despite 403"


# ── Summary endpoint auth tests ───────────────────────────────────────────────


@pytest.mark.integration
async def test_summary_caller_is_null_for_anonymous(
    client: httpx.AsyncClient,
    alice_token: str,
) -> None:
    """
    ``GET /v1/posts/{id}/summary`` returns ``"caller": null`` for anonymous.

    Exercises the ``get_request_context().auth.user_id`` path inside
    ``PostRouter.get_summary()``.
    """
    create_resp = await client.post(
        "/v1/posts",
        json={"title": "Summary anon test", "body": "Body"},
        headers={"Authorization": f"Bearer {alice_token}"},
    )
    assert create_resp.status_code == 201
    pk = create_resp.json()["pk"]

    # Anonymous GET — no auth header.
    summary_resp = await client.get(f"/v1/posts/{pk}/summary")
    assert summary_resp.status_code == 200, summary_resp.text
    assert summary_resp.json()["caller"] is None


@pytest.mark.integration
async def test_summary_caller_reflects_authenticated_user(
    client: httpx.AsyncClient,
    alice_token: str,
) -> None:
    """
    ``GET /v1/posts/{id}/summary`` returns ``caller`` = alice's user_id when
    an authenticated request is made.

    Exercises:
    - ``RequestContextMiddleware`` sets the ``AuthContext`` ContextVar.
    - ``get_request_context().auth.user_id`` resolves inside the custom handler.
    """
    create_resp = await client.post(
        "/v1/posts",
        json={"title": "Summary auth test", "body": "Body"},
        headers={"Authorization": f"Bearer {alice_token}"},
    )
    assert create_resp.status_code == 201
    pk = create_resp.json()["pk"]

    summary_resp = await client.get(
        f"/v1/posts/{pk}/summary",
        headers={"Authorization": f"Bearer {alice_token}"},
    )
    assert summary_resp.status_code == 200, summary_resp.text
    assert summary_resp.json()["caller"] == "00000000-0000-0000-0000-000000000001"


__all__ = []
