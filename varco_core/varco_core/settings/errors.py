"""
varco_core.settings.errors
============================
Error taxonomy for ``bind_settings()`` (Plan 048 / DI-6, §D-eager).

``SettingsError`` is the base; ``SettingsValidationError`` is raised when a
class fails to construct from the environment (fail-fast, at ``bootstrap()``
time — brief 015 §Evidence Gap 4's "synchronous with registration" property);
``SettingsAlreadyBoundError`` is raised when a second ``instance=`` call
targets a ``(cls, qualifier)`` pair a previous ``bind_settings()`` call
already registered non-fallback.

DESIGN: ``SettingsAlreadyBoundError`` subclasses *both* ``SettingsError`` and
``varco_core.di.AlreadyBoundError``
    ✅ ``except SettingsError`` catches every error this module raises
       (validation and re-binding alike) — the uniform "something is wrong
       with this settings class" catch site the recipe promises.
    ✅ ``except varco_core.di.AlreadyBoundError`` — the existing "a second
       non-fallback binding of the same (interface, qualifier) was
       attempted" catch site every other ``bind_*``/``enable_*`` helper in
       the tree already raises — already documented there as *the* raise
       for "calling twice replaces the previous binding" bugs
       (``varco_core/di/__init__.py``'s ``AlreadyBoundError`` docstring).
       A settings class re-bind is exactly that failure mode; giving it a
       second, disconnected exception type would defeat a caller who
       already catches the general one.
    ❌ Two base classes to keep in sync if either's ``__init__`` signature
       ever changes — mitigated by keeping this class's ``__init__`` a thin
       message-only wrapper, never forwarding args to either base beyond
       the message string.

Thread safety:  N/A — plain exception types, no shared state.
Async safety:   N/A — never awaited, never held across an ``await``.
"""

from __future__ import annotations

from varco_core.di import AlreadyBoundError


class SettingsError(ValueError):
    """Base class for every error ``varco_core.settings`` raises."""


class SettingsValidationError(SettingsError):
    """
    Raised when a settings class fails to construct from the environment.

    The message names, per invalid field: the environment variable name
    (never the raw offending value — a secret must not land in a
    traceback), the pydantic error message, and the settings class name.
    """

    def __init__(self, cls: type, lines: list[str]) -> None:
        """
        Args:
            cls: The settings class that failed to construct.
            lines: One human-readable line per invalid field, each already
                containing the env var name and the pydantic message but
                never the raw value.
        """
        joined = "; ".join(lines)
        super().__init__(f"{cls.__name__} failed to construct from the environment: {joined}")
        self.cls = cls
        self.lines = tuple(lines)


class SettingsAlreadyBoundError(SettingsError, AlreadyBoundError):
    """
    Raised when ``bind_settings(container, Cls, instance=...)`` targets a
    ``(Cls, qualifier)`` pair a previous ``bind_settings()`` call already
    bound non-fallback.

    A second code override is a bug, not a precedence question — tests
    replace a binding with providify's ``di_overrides`` fixture instead of
    a second ``bind_settings()`` call (CLAUDE.md's RL-3 rule).
    """

    def __init__(self, cls: type, qualifier: str | None) -> None:
        """
        Args:
            cls: The settings class already bound.
            qualifier: The qualifier the existing binding was registered
                under, or ``None`` for the unqualified binding.
        """
        q = f" (qualifier={qualifier!r})" if qualifier is not None else ""
        super().__init__(f"{cls.__name__}{q} is already bound — use di_overrides in tests")
        self.cls = cls
        self.qualifier = qualifier


__all__ = [
    "SettingsAlreadyBoundError",
    "SettingsError",
    "SettingsValidationError",
]
