"""
fastrest_core.authority.sources.pem_file
==========================================

``PemFileSource`` — loads a single PEM file (private or public key) and
exposes its public key material as a ``JsonWebKeySet`` for token verification.

DESIGN: frozen dataclass + async load/refresh
    ✅ Immutable after construction — safe to share across async tasks.
    ✅ Always re-reads file on refresh() — picks up operator-rotated certs
       without a process restart.
    ✅ Works with both private PEM (PKCS#8 / traditional) and public PEM —
       JwkBuilder.from_pem() auto-detects and strips private material.
    ❌ No file-watch mechanism — refresh() is only triggered by a kid-not-found
       signal from the registry, not by inotify / kqueue.  For instant
       propagation, use PemFolderSource with a polling registry.

Thread safety:  ✅ Immutable fields; file reads are independent operations.
Async safety:   ✅ load() and refresh() use asyncio.to_thread() for file I/O
                   so the event loop is never blocked.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

from fastrest_core.jwk.builder import JwkBuilder
from fastrest_core.jwk.model import JsonWebKeySet

from fastrest_core.authority.exceptions import KeyLoadError


# ── PemFileSource ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class PemFileSource:
    """
    Key source that reads a single PEM file from disk.

    The ``kid`` value is explicit — the caller decides it.  A natural default
    is ``Path(path).stem`` (filename without extension), which is what
    ``IssuerSourceFactory.from_string("pem::/path/to/file.pem")`` uses.

    Supports both private-key PEMs (PKCS#8 ``BEGIN PRIVATE KEY``,
    ``BEGIN RSA/EC PRIVATE KEY``) and public-key PEMs (``BEGIN PUBLIC KEY``).
    Private material is always stripped — only the public key is exposed in
    the returned ``JsonWebKeySet``.

    Thread safety:  ✅ Frozen — safe to share across threads and tasks.
    Async safety:   ✅ File I/O is delegated to asyncio.to_thread().

    Attributes:
        path:      Path to the PEM file.
        kid:       Key ID to assign to the loaded key.
        algorithm: JWT signing algorithm (e.g. ``"RS256"``, ``"ES256"``).
        use:       JWK public key use — ``"sig"`` (default) or ``"enc"``.

    Edge cases:
        - If the PEM file contains a private key, only the public portion is
          returned — private material is never included in the keyset.
        - File not found or permission denied → ``KeyLoadError`` (chained).
        - Malformed PEM → ``KeyLoadError`` (chained from cryptography's
          ``ValueError``).
        - refresh() always re-reads the file — if the file hasn't changed,
          this is a no-op at the keyset level (same content, new object).

    Example::

        source = PemFileSource(
            path=Path("/etc/certs/my-service.pem"),
            kid="my-service:auth-2025",
            algorithm="RS256",
        )
        keyset = await source.load()
        # keyset.keys[0].kid == "my-service:auth-2025"
    """

    path: Path
    kid: str
    algorithm: str
    # "sig" is the only value used for JWT signing keys; "enc" is for
    # encryption keys — kept configurable for completeness.
    use: str = "sig"

    @property
    def source_id(self) -> str:
        """Stable identifier: ``pem::<absolute path>``."""
        return f"pem::{self.path}"

    async def load(self) -> JsonWebKeySet:
        """
        Read the PEM file and return a single-key ``JsonWebKeySet``.

        Wraps synchronous file I/O in ``asyncio.to_thread()`` to avoid
        blocking the event loop.

        Returns:
            ``JsonWebKeySet`` containing exactly one public ``JsonWebKey``.

        Raises:
            KeyLoadError: File not found, permission denied, or the PEM is
                          malformed / contains an unsupported key type.

        Edge cases:
            - Called multiple times → re-reads the file each time (idempotent
              but not cached — use the keyset returned by the registry).
        """
        # Delegate to sync helper in a thread — file reads are blocking I/O
        return await asyncio.to_thread(self._read_and_build)

    async def refresh(self) -> JsonWebKeySet:
        """
        Re-read the PEM file to pick up operator-rotated certificates.

        Called by the registry when a ``kid``-not-found signal is received.
        Since PEM files are typically updated by replacing the file entirely
        (not in-place), re-reading always gives the latest key material.

        Returns:
            Fresh ``JsonWebKeySet`` from the current file contents.

        Raises:
            KeyLoadError: Same conditions as ``load()``.

        Edge cases:
            - No rate-limiting on refresh() — this is intentional for PEM
              files since re-reads are cheap (sub-millisecond local I/O).
              Rate-limiting is only needed for network sources.
        """
        # For a static file source, refresh == load — there is no cache to
        # invalidate; just re-read the file unconditionally.
        return await self.load()

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _read_and_build(self) -> JsonWebKeySet:
        """
        Synchronous implementation of PEM read + JWK build.

        Separated from load()/refresh() so it can run in a thread pool via
        asyncio.to_thread() without the async overhead inside the thread.

        Returns:
            ``JsonWebKeySet`` with one public key entry.

        Raises:
            KeyLoadError: Wraps OSError (file not found / permissions) or
                          ValueError (malformed PEM) with context.
        """
        try:
            pem_bytes = Path(self.path).read_bytes()
        except OSError as e:
            raise KeyLoadError(
                f"Cannot read PEM file at {self.path!r}: {e}. "
                f"Check that the file exists and is readable by the process."
            ) from e

        try:
            # include_private=False is the default — we never expose private
            # material through the JWKS endpoint.
            jwk = JwkBuilder.from_pem(
                pem_bytes,
                kid=self.kid,
                use=self.use,
                alg=self.algorithm,
            )
        except (ValueError, TypeError) as e:
            raise KeyLoadError(
                f"PEM file at {self.path!r} could not be parsed as an RSA or EC key: {e}. "
                f"Supported formats: PKCS#8 private key, traditional RSA/EC private key, "
                f"or SubjectPublicKeyInfo public key."
            ) from e

        # public_key() strips private material — safe for JWKS exposure.
        return JwkBuilder.keyset(jwk.public_key())

    def __repr__(self) -> str:
        return (
            f"PemFileSource("
            f"path={str(self.path)!r}, "
            f"kid={self.kid!r}, "
            f"algorithm={self.algorithm!r})"
        )
