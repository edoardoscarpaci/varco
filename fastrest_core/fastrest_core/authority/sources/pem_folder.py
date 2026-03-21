"""
fastrest_core.authority.sources.pem_folder
============================================

``PemFolderSource`` — scans a directory for ``*.pem`` files and exposes
all their public keys as a ``JsonWebKeySet``.

The ``kid`` for each key is derived from the PEM filename stem, e.g.:
    ``auth-2025-A.pem`` → kid ``"auth-2025-A"``

Key rotation works by placing a new file in the folder.  On ``refresh()``,
the source compares current file mtimes against the last-seen state and
re-scans only when something changed.

DESIGN: mutable class over frozen dataclass
    ✅ Must track per-file mtimes across calls — mutable state is unavoidable.
    ✅ _scan() is idempotent w.r.t. external state — given the same files,
       it always produces the same keyset.
    ❌ Not immutable — callers must not share a single instance across
       concurrent tasks without understanding the safety contract below.

Thread safety:  ⚠️ Conditional — _mtimes and _keyset are mutable.
                    Concurrent load()/refresh() calls may race.  In practice,
                    the registry serialises calls through its own lock, so
                    this is safe in the expected usage pattern.  If you share
                    a PemFolderSource across tasks directly, add external
                    synchronisation.
Async safety:   ✅ _scan() runs in asyncio.to_thread() — file I/O never
                   blocks the event loop.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from fastrest_core.jwk.builder import JwkBuilder
from fastrest_core.jwk.model import JsonWebKey, JsonWebKeySet

from fastrest_core.authority.exceptions import KeyLoadError


# ── PemFolderSource ───────────────────────────────────────────────────────────


class PemFolderSource:
    """
    Key source that watches a folder for ``*.pem`` files.

    Each PEM file in the folder contributes one key to the returned
    ``JsonWebKeySet``.  The ``kid`` for each key is the filename stem
    (filename without the ``.pem`` extension), so naming the file after
    the kid is the convention:

    .. code-block:: text

        /etc/certs/
            user:auth-2025-A.pem   → kid = "user:auth-2025-A"
            user:auth-2025-B.pem   → kid = "user:auth-2025-B"

    Rotation
    --------
    Drop a new ``.pem`` file into the folder.  On the next ``refresh()``
    call (triggered by a kid-not-found signal), the folder is re-scanned
    and the new key is added to the keyset.

    Retirement
    ----------
    Remove the old ``.pem`` file from the folder.  On the next
    ``refresh()``, the key is absent from the returned keyset.

    Thread safety:  ⚠️ See module docstring.
    Async safety:   ✅ All I/O runs via asyncio.to_thread().

    Args:
        path:      Path to the directory containing PEM files.
        algorithm: JWT signing algorithm for all keys in this folder
                   (e.g. ``"RS256"``).  All files must use the same
                   algorithm — use separate folders for mixed algorithms.
        use:       JWK public key use — ``"sig"`` (default) or ``"enc"``.

    Edge cases:
        - Empty folder → empty ``JsonWebKeySet`` (valid, not an error).
        - Non-PEM files in the folder are silently ignored.
        - A PEM file that fails to parse → ``KeyLoadError`` with the
          filename in the message.
        - Symlinks to PEM files are followed (``Path.glob()`` behaviour).
        - refresh() is a no-op (returns cached keyset) when no file mtimes
          have changed — avoids unnecessary re-reads.

    Example::

        source = PemFolderSource(
            path=Path("/etc/certs/user-keys"),
            algorithm="RS256",
        )
        keyset = await source.load()
        # Contains one JsonWebKey per *.pem file found
    """

    # __slots__ saves ~200 bytes per instance and prevents accidental
    # attribute addition outside __init__.
    __slots__ = ("_path", "_algorithm", "_use", "_mtimes", "_keyset")

    def __init__(self, path: Path | str, *, algorithm: str, use: str = "sig") -> None:
        """
        Args:
            path:      Directory path.  Converted to ``Path`` if a string.
            algorithm: Signing algorithm applied to all keys in the folder.
            use:       Public key use — ``"sig"`` or ``"enc"``.

        Raises:
            ValueError: ``path`` is not a directory (checked lazily on
                        first load() call, not here — avoids racy startup).
        """
        self._path: Path = Path(path)
        self._algorithm: str = algorithm
        self._use: str = use

        # Tracks per-file mtime at last scan.  Used by _has_changes() to
        # detect whether a re-scan is needed without reading file contents.
        self._mtimes: dict[Path, float] = {}

        # Cached keyset from the last successful scan.  None until first load().
        self._keyset: JsonWebKeySet | None = None

    @property
    def source_id(self) -> str:
        """Stable identifier: ``pem-folder::<absolute path>``."""
        return f"pem-folder::{self._path}"

    async def load(self) -> JsonWebKeySet:
        """
        Scan the folder and build a keyset from all ``*.pem`` files.

        Always performs a full scan regardless of cached state.

        Returns:
            ``JsonWebKeySet`` with one entry per valid PEM file found.

        Raises:
            KeyLoadError: Folder does not exist, is not readable, or a PEM
                          file fails to parse.

        Edge cases:
            - Returns an empty keyset for an empty folder — not an error.
        """
        return await asyncio.to_thread(self._scan)

    async def refresh(self) -> JsonWebKeySet:
        """
        Re-scan the folder only if any file's mtime has changed.

        Checks file mtimes (cheap ``os.stat`` calls) before committing to
        re-reading all PEM files.  Returns the cached keyset immediately
        when nothing has changed.

        Returns:
            Up-to-date ``JsonWebKeySet`` — either freshly scanned or cached.

        Raises:
            KeyLoadError: Same conditions as ``load()``.

        Edge cases:
            - A file whose content changed but mtime did not → NOT detected.
              This is acceptable because PEM rotation always involves a new
              file or an mtime update in practice.
            - New files added since last scan → detected via mtime dict
              comparison (new key ∉ self._mtimes).
        """
        # _has_changes() is cheap (stat calls, no file reads) — run in thread
        # to avoid blocking the loop on slow filesystems (NFS, etc.)
        changed = await asyncio.to_thread(self._has_changes)
        if not changed:
            # Nothing modified since last scan — return cached result
            return self._keyset or JsonWebKeySet(keys=())
        return await asyncio.to_thread(self._scan)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _has_changes(self) -> bool:
        """
        Compare current directory state against the last-seen mtime snapshot.

        Returns ``True`` when any of the following are true:
        - A new ``*.pem`` file appeared in the folder.
        - An existing file's mtime changed (content may have been updated).
        - A previously seen file was deleted.

        Returns:
            ``True`` when a re-scan is warranted; ``False`` when cache is fresh.
        """
        try:
            # Build the current mtime map — stat() each found PEM file
            current: dict[Path, float] = {
                p: p.stat().st_mtime for p in self._path.glob("*.pem")
            }
        except OSError:
            # If we can't even stat the directory, treat as changed so the
            # next scan will produce a proper KeyLoadError with context.
            return True

        return current != self._mtimes

    def _scan(self) -> JsonWebKeySet:
        """
        Synchronous implementation — reads all PEM files and builds keyset.

        Updates ``self._mtimes`` and ``self._keyset`` as side effects so that
        ``refresh()`` can skip the re-scan when nothing changed.

        Returns:
            Fresh ``JsonWebKeySet``.

        Raises:
            KeyLoadError: Folder not found / not a directory, or a PEM fails.
        """
        if not self._path.exists():
            raise KeyLoadError(
                f"PEM folder not found at {self._path!r}. "
                f"Create the directory and place *.pem key files inside it."
            )
        if not self._path.is_dir():
            raise KeyLoadError(
                f"{self._path!r} is not a directory. "
                f"pem-folder:: source requires a directory path, not a file path. "
                f"Use pem:: for a single file."
            )

        keys: list[JsonWebKey] = []
        new_mtimes: dict[Path, float] = {}

        # Sort for deterministic ordering — JWKS key order is stable
        for pem_file in sorted(self._path.glob("*.pem")):
            # Use the filename stem as kid — convention: name the file after
            # the kid you want (e.g. "user:auth-2025-A.pem")
            kid = pem_file.stem

            try:
                pem_bytes = pem_file.read_bytes()
            except OSError as e:
                raise KeyLoadError(
                    f"Cannot read PEM file {pem_file.name!r} in folder {self._path!r}: {e}."
                ) from e

            try:
                jwk = JwkBuilder.from_pem(
                    pem_bytes,
                    kid=kid,
                    use=self._use,
                    alg=self._algorithm,
                )
            except (ValueError, TypeError) as e:
                raise KeyLoadError(
                    f"PEM file {pem_file.name!r} in folder {self._path!r} could not be parsed: {e}. "
                    f"Ensure the file contains a valid RSA or EC key."
                ) from e

            keys.append(jwk.public_key())
            # Record mtime after successful read — stale mtime causes spurious
            # re-scan on next refresh(), which is acceptable.
            new_mtimes[pem_file] = pem_file.stat().st_mtime

        # Commit new state only after the full scan succeeds —
        # partial failure leaves the old mtime snapshot intact.
        self._mtimes = new_mtimes
        self._keyset = JsonWebKeySet(keys=tuple(keys))
        return self._keyset

    def __repr__(self) -> str:
        key_count = len(self._keyset.keys) if self._keyset is not None else 0
        return (
            f"PemFolderSource("
            f"path={str(self._path)!r}, "
            f"algorithm={self._algorithm!r}, "
            f"loaded_keys={key_count})"
        )
