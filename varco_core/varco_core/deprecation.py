"""
One deprecation mechanism for the whole workspace (Plan 022 / §D-DEP).

Two public helpers:

* :func:`deprecated` — decorate a function, coroutine function or class whose
  *name is staying* but whose use is discouraged.
* :func:`deprecated_alias` — build a PEP 562 module ``__getattr__`` so an
  **old name** keeps resolving, to the identical object, after a rename.

Both force ``removed_in=`` at authoring time. That is the single discipline an
ad-hoc ``warnings.warn`` call cannot enforce, and it makes every scheduled
removal greppable::

    rg 'removed_in="3\\.' varco_*/varco_*

DESIGN: one shared mechanism now, written policy later (RL-9)
  ✅ Replaces ad-hoc ``warnings.warn`` deprecation sites with one concept, so
     category, message shape and ``stacklevel`` cannot drift per call site.
  ✅ ``removed_in`` is a *required* keyword. A deprecation that never names its
     removal version is how aliases rot into permanent API.
  ✅ ``stacklevel`` is tuned so the warning blames the **caller**. A warning
     attributed to this module tells the reader nothing they can act on.
  ✅ RL-9 (SemVer/deprecation policy) needs this regardless; building it here
     means RL-9 writes prose rather than code.
  ❌ Adds a public symbol during an audit whose purpose is to *shrink* the
     public surface. Accepted: one module, two functions, and it is the
     mechanism that makes every other removal cheap.

DESIGN: rejected — PEP 702 ``warnings.deprecated``
  ✅ Type-checker-visible, stdlib-blessed, and the obvious long-term answer.
  ❌ It is in stdlib ``warnings`` only from **Python 3.13**, and every package
     here is ``requires-python = ">=3.12"``. Adopting it today means taking
     ``typing_extensions`` as a *runtime* dependency — the identical trade
     Plan 021 rejected for PEP 696, for the identical reason.
  → Intended migration: replace this module's internals with
    ``warnings.deprecated`` the day the floor moves to 3.13. The public
    surface here (``since``/``removed_in``/``replacement``) is deliberately a
    superset of PEP 702's, so that migration is internal-only.

DESIGN: rejected — a subclass-based alias (``class Old(New): ...``)
  ✅ Would let the alias carry its own ``__init__`` warning with no wrapping.
  ❌ ``Old is not New``, so ``except Old`` would stop catching something raised
     as ``New`` and ``isinstance`` would go asymmetric — the exact breakage an
     alias exists to prevent. :func:`deprecated_alias` therefore returns the
     **identical object**, never a subclass.

Thread safety: ✅ Both helpers are pure factories; the wrappers they build add
no shared mutable state. ``warnings.warn`` is itself thread-safe.
Async safety:  ✅ :func:`deprecated` detects a coroutine function and builds an
``async def`` wrapper, so ``inspect.iscoroutinefunction`` still holds and the
result stays awaitable.
"""

from __future__ import annotations

import dataclasses
import functools
import inspect
import sys
import threading
import warnings
from typing import TYPE_CHECKING, Any, TypeVar, cast

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = [
    "DeprecationRecord",
    "deprecated",
    "deprecated_alias",
    "fired_deprecations",
    "reset_fired_deprecations",
]

_T = TypeVar("_T")


@dataclasses.dataclass(frozen=True)
class DeprecationRecord:
    """
    One deprecated subject's cumulative firing history (Plan 052 / §D-deprecations).

    A single record per ``subject`` accumulates ``count`` across every call —
    the ledger is a dedup, not a log. ``first_location`` is fixed at the first
    fire and never updated by later ones.

    Args:
        subject:       What is deprecated, as rendered in the warning message.
        since:         Version the deprecation began, or ``None`` for a direct
                       ``warnings.warn(DeprecationWarning)`` site this module
                       does not own (Plan 052's eight direct sites).
        removed_in:    Version the subject is removed in, or ``None`` (ditto).
        replacement:   Preferred replacement, or ``None`` if there is none.
        count:         Number of times this subject has fired since the last
                       :func:`reset_fired_deprecations` call.
        first_location: ``"<file>:<lineno>"`` of the first call site observed.
    """

    subject: str
    since: str | None
    removed_in: str | None
    replacement: str | None
    count: int
    first_location: str


#: Process-global ledger, keyed by subject (dedup key — see DeprecationRecord).
#: DESIGN: a plain dict under a sync `threading.Lock`, not an `asyncio.Lock`
#! ✅ `warnings.warn` itself is synchronous and may be called from any thread;
#!    the lazy-`asyncio.Lock` rule (CLAUDE.md) is about event-loop binding,
#!    which does not apply to a plain mutex guarding a dict.
#! ✅ Module-level construction is correct here — a `threading.Lock` has no
#!    running-event-loop requirement, unlike `asyncio.Lock`.
#! ❌ Process-global, inherently — reset only in tests (`reset_fired_deprecations`).
_FIRED: dict[str, DeprecationRecord] = {}
_FIRED_LOCK = threading.Lock()


def _note(
    subject: str,
    *,
    since: str | None,
    removed_in: str | None,
    replacement: str | None,
) -> None:
    """
    Record one firing of ``subject`` in the process-global ledger.

    Called immediately **before** each ``warnings.warn(DeprecationWarning)``
    site in this module, so the ledger captures a fire regardless of the
    active warnings filter (a ``-W ignore::DeprecationWarning`` process still
    gets a record — that is the entire reason this hook exists).

    Args:
        subject:     Dedup key — the deprecated name as rendered in the message.
        since:       Version the deprecation began, or ``None``.
        removed_in:  Version of removal, or ``None``.
        replacement: Preferred replacement, or ``None``.

    Thread safety: guarded by ``_FIRED_LOCK`` — safe to call concurrently from
    multiple threads (exercised by the ledger's own thread-safety test).
    """
    # frame 0 = this function, frame 1 = the one-level wrapper that called us,
    # frame 2 = whoever called the deprecated thing — the caller we want to blame.
    frame = sys._getframe(2)
    location = f"{frame.f_code.co_filename}:{frame.f_lineno}"
    with _FIRED_LOCK:
        existing = _FIRED.get(subject)
        if existing is None:
            _FIRED[subject] = DeprecationRecord(
                subject=subject,
                since=since,
                removed_in=removed_in,
                replacement=replacement,
                count=1,
                first_location=location,
            )
        else:
            _FIRED[subject] = dataclasses.replace(existing, count=existing.count + 1)


def fired_deprecations() -> tuple[DeprecationRecord, ...]:
    """
    Snapshot the process-global deprecation ledger.

    Returns:
        A tuple of :class:`DeprecationRecord`, one per distinct subject that
        has fired since the last :func:`reset_fired_deprecations` call (or
        process start). Order is insertion order (first-fired-first).
    """
    with _FIRED_LOCK:
        return tuple(_FIRED.values())


def reset_fired_deprecations() -> None:
    """
    Empty the process-global deprecation ledger.

    Test-only. Production code should never call this — the ledger is
    documented as "since process start" (varco diagnose's report footnote).
    """
    with _FIRED_LOCK:
        _FIRED.clear()


#: ``stacklevel`` for a warning raised inside a one-level wrapper: 1 is the
#: wrapper itself, 2 is whoever called it. Every warn() below is exactly one
#: frame deep, so this constant is correct for all of them.
_CALLER_STACKLEVEL = 2


def _build_message(
    subject: str,
    *,
    since: str,
    removed_in: str,
    replacement: str | None,
) -> str:
    """
    Render the one canonical deprecation sentence.

    Args:
        subject:     What is deprecated, as the reader would write it.
        since:       Version in which the deprecation began.
        removed_in:  Version in which ``subject`` stops existing.
        replacement: What to use instead, or ``None`` if there is no direct
                     replacement.

    Returns:
        A single-sentence message naming all supplied metadata. Every value is
        interpolated verbatim so it stays greppable in captured logs.
    """
    tail = f" Use {replacement} instead." if replacement else ""
    return f"{subject} is deprecated since {since} and will be removed in {removed_in}.{tail}"


def deprecated(
    *,
    since: str,
    removed_in: str,
    replacement: str | None = None,
    name: str | None = None,
) -> Callable[[_T], _T]:
    """
    Mark a function, coroutine function or class as deprecated.

    The decorated object keeps its identity: functions are wrapped with
    ``functools.wraps`` (so ``__name__``/``__doc__``/``__wrapped__`` survive)
    and classes are mutated **in place** rather than subclassed, so
    ``isinstance`` and ``type(x) is Cls`` continue to hold.

    Args:
        since:       Version in which the deprecation began, e.g. ``"3.0.0"``.
        removed_in:  Version in which the symbol will be removed. Required —
                     see this module's DESIGN block.
        replacement: Name of the preferred replacement, if there is one.
        name:        Override for the deprecated name used in the message.
                     Needed when building a **deprecated alias of a renamed
                     function**: ``functools.wraps`` copies the *new* name onto
                     the wrapper, so without this the message would name the
                     replacement twice and never mention the name the caller
                     actually typed. Defaults to the decorated object's own
                     ``__name__``.

    Returns:
        A decorator that returns the same callable/class, instrumented to emit
        a :class:`DeprecationWarning` attributed to its caller.

    Raises:
        TypeError: If ``since`` or ``removed_in`` is omitted (they are
            required keyword-only arguments).

    Edge cases:
        - **Classes warn on instantiation, not at decoration.** Warning at
          import time is unactionable noise for anyone who merely imports the
          module without using the symbol.
        - A class that defines no ``__init__`` of its own is still handled —
          the inherited ``__init__`` is what gets wrapped.
        - A coroutine function stays a coroutine function; the warning fires
          when it is *called*, not when the coroutine is awaited.
        - Decorating the same object twice produces two warnings per call. Do
          not stack it.

    Example:
        >>> @deprecated(since="3.0.0", removed_in="4.0.0", replacement="render_rls_ddl")
        ... def enable_rls_ddl() -> list[str]:
        ...     return []
    """

    def decorate(subject: _T) -> _T:
        if isinstance(subject, type):
            return cast(  # the helper returns the very same object it was given
                "_T",
                _deprecate_class(
                    subject,
                    since=since,
                    removed_in=removed_in,
                    replacement=replacement,
                    name=name,
                ),
            )
        return cast(  # ditto: a wraps()-preserving wrapper of the same shape
            "_T",
            _deprecate_callable(
                subject,
                since=since,
                removed_in=removed_in,
                replacement=replacement,
                name=name,
            ),
        )

    return decorate


def _deprecate_class(
    cls: Any,
    *,
    since: str,
    removed_in: str,
    replacement: str | None,
    name: str | None = None,
) -> Any:
    """
    Instrument ``cls.__init__`` in place so instantiation warns.

    Mutating the class rather than returning a subclass is what preserves
    ``type(instance) is cls`` — see this module's rejected-alternative block.

    Args:
        cls:         The class to deprecate.
        since:       Version in which the deprecation began.
        removed_in:  Version in which the class will be removed.
        replacement: Preferred replacement name, if any.
        name:        Override for the deprecated name in the message.

    Returns:
        The very same class object, with a wrapped ``__init__``.
    """
    subject = name or cls.__name__
    message = _build_message(
        subject,
        since=since,
        removed_in=removed_in,
        replacement=replacement,
    )
    original_init = cls.__init__

    @functools.wraps(original_init)
    def __init__(self: Any, *args: Any, **kwargs: Any) -> None:
        _note(subject, since=since, removed_in=removed_in, replacement=replacement)
        warnings.warn(message, DeprecationWarning, stacklevel=_CALLER_STACKLEVEL)
        original_init(self, *args, **kwargs)

    cls.__init__ = __init__
    return cls


def _deprecate_callable(
    fn: Any,
    *,
    since: str,
    removed_in: str,
    replacement: str | None,
    name: str | None = None,
) -> Any:
    """
    Wrap a function or coroutine function so calling it warns.

    Args:
        fn:          The callable to deprecate.
        since:       Version in which the deprecation began.
        removed_in:  Version in which the callable will be removed.
        replacement: Preferred replacement name, if any.
        name:        Override for the deprecated name in the message.

    Returns:
        A ``functools.wraps``-preserving wrapper of the same async-ness as
        ``fn``, so ``inspect.iscoroutinefunction`` is unchanged.
    """
    subject = name or str(getattr(fn, "__name__", repr(fn)))
    message = _build_message(
        subject,
        since=since,
        removed_in=removed_in,
        replacement=replacement,
    )

    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            _note(subject, since=since, removed_in=removed_in, replacement=replacement)
            warnings.warn(message, DeprecationWarning, stacklevel=_CALLER_STACKLEVEL)
            return await fn(*args, **kwargs)

        return async_wrapper

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        _note(subject, since=since, removed_in=removed_in, replacement=replacement)
        warnings.warn(message, DeprecationWarning, stacklevel=_CALLER_STACKLEVEL)
        return fn(*args, **kwargs)

    return wrapper


def deprecated_alias(
    name: str,
    target: object,
    *,
    since: str,
    removed_in: str,
    fallback: Callable[[str], object] | None = None,
) -> Callable[[str], object]:
    """
    Build a PEP 562 module ``__getattr__`` serving one renamed symbol.

    Assign the result to a module's ``__getattr__`` so the **old** name keeps
    resolving — to the *identical* object, never a subclass — while emitting a
    :class:`DeprecationWarning` attributed to the importing caller::

        from varco_core.deprecation import deprecated_alias
        from varco_core.migration.errors import SchemaMigrationError

        __getattr__ = deprecated_alias(
            "MigrationError", SchemaMigrationError, since="3.0.0", removed_in="4.0.0"
        )

    Chain ``fallback=`` to serve more than one alias from a single module,
    innermost alias first::

        __getattr__ = deprecated_alias(
            "MigrationPlan", SchemaMigrationPlan, since="3.0.0", removed_in="4.0.0",
            fallback=deprecated_alias(
                "MigrationError", SchemaMigrationError,
                since="3.0.0", removed_in="4.0.0",
            ),
        )

    Args:
        name:        The old, deprecated attribute name.
        target:      The object the old name must keep resolving to. Returned
                     by identity — ``old is new`` — so ``except``/``isinstance``
                     are unaffected by the rename.
        since:       Version in which the deprecation began.
        removed_in:  Version in which the old name stops resolving. Required.
        fallback:    Another ``__getattr__``-shaped callable to consult for
                     any name this one does not serve. Use it to chain
                     several aliases in one module.

    Returns:
        A ``(name: str) -> object`` callable suitable for assignment to a
        module-level ``__getattr__``.

    Raises:
        AttributeError: From the returned callable, for any name that is
            neither ``name`` nor served by ``fallback``. Preserving normal
            ``AttributeError`` semantics is what keeps ``hasattr`` honest —
            a ``__getattr__`` returning ``None`` for typos would break every
            ``hasattr`` check reaching that module.
        TypeError: If ``since`` or ``removed_in`` is omitted.

    Edge cases:
        - The warning fires on **attribute access**, which for a
          ``from module import OldName`` is import time. That is correct: the
          import site is the thing that needs editing.
        - Module ``__getattr__`` is consulted only for names *not* found
          normally, so an alias never shadows a real module attribute.
        - ``dir(module)`` will not list the alias. That is intentional — a
          deprecated name should not be advertised by autocomplete.
    """
    target_name = getattr(target, "__name__", None)
    alias_replacement = str(target_name) if target_name is not None else None
    message = _build_message(
        name,
        since=since,
        removed_in=removed_in,
        replacement=alias_replacement,
    )

    def module_getattr(requested: str) -> object:
        if requested == name:
            _note(name, since=since, removed_in=removed_in, replacement=alias_replacement)
            warnings.warn(message, DeprecationWarning, stacklevel=_CALLER_STACKLEVEL)
            return target
        if fallback is not None:
            return fallback(requested)
        raise AttributeError(requested)

    return module_getattr
