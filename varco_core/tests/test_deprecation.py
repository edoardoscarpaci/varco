"""
Red tests for ``varco_core.deprecation`` (Plan 022 / Phase 2, step 11 — §D-DEP).

The module does not exist yet: every test here must fail with an
``ImportError``/``AttributeError`` until ``deprecated()`` and
``deprecated_alias()`` land.

Contract under test (§D-DEP):
    * category is ``DeprecationWarning``
    * ``stacklevel`` points at the *caller*, not at the decorator
    * ``since`` / ``removed_in`` / ``replacement`` all appear in the message
    * ``removed_in`` is a required keyword-only argument
    * decorated callables keep their identity (``functools.wraps``) and value
    * an alias IS the target object — ``isinstance`` / ``except`` keep working
"""

from __future__ import annotations

import inspect
import warnings

import pytest

# ── fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def deprecation_module():
    """
    Import the not-yet-existing module.

    Assumed mechanism for ``deprecated_alias``: it returns a module-level
    ``__getattr__``-shaped callable (``(name: str) -> object``), because
    AB-2 needs attribute-access-time deprecation, which PEP 562 module
    ``__getattr__`` is the only way to get. The implementer is free to pick a
    different shape, but these tests pin the behaviour that shape must have.
    """
    import varco_core.deprecation as module  # noqa: PLC0415

    return module


@pytest.fixture
def deprecated(deprecation_module):
    return deprecation_module.deprecated


@pytest.fixture
def deprecated_alias(deprecation_module):
    return deprecation_module.deprecated_alias


# ── category / message content ────────────────────────────────────────────────


def test_decorated_function_emits_deprecation_warning(deprecated) -> None:
    """The category must be DeprecationWarning — not UserWarning, not FutureWarning."""

    @deprecated(since="3.0.0", removed_in="5.0.0", replacement="new_fn")
    def old_fn() -> int:
        return 7

    with pytest.warns(DeprecationWarning):
        old_fn()


def test_warning_message_names_since_removed_in_and_replacement(deprecated) -> None:
    """All three metadata values must be greppable in the emitted text (RL-9 relies on it)."""

    @deprecated(since="3.0.0", removed_in="5.0.0", replacement="render_rls_ddl")
    def old_fn() -> int:
        return 7

    with pytest.warns(DeprecationWarning) as record:
        old_fn()

    message = str(record[0].message)
    assert "3.0.0" in message
    assert "5.0.0" in message
    assert "render_rls_ddl" in message
    assert "old_fn" in message


def test_removed_in_is_a_required_keyword_argument(deprecated) -> None:
    """§D-DEP: forcing removed_in at authoring time is the one discipline ad-hoc warn cannot."""
    with pytest.raises(TypeError):
        deprecated(since="3.0.0", replacement="new_fn")  # type: ignore[call-arg]


def test_deprecated_is_keyword_only(deprecated) -> None:
    """Positional args would make the three metadata values order-dependent and unreadable."""
    signature = inspect.signature(deprecated)
    for name in ("since", "removed_in", "replacement"):
        assert signature.parameters[name].kind is inspect.Parameter.KEYWORD_ONLY


# ── stacklevel ────────────────────────────────────────────────────────────────


def test_stacklevel_points_at_the_caller_not_the_decorator(deprecated) -> None:
    """A warning attributed to varco_core/deprecation.py is useless — it must blame this file."""

    @deprecated(since="3.0.0", removed_in="5.0.0", replacement="new_fn")
    def old_fn() -> int:
        return 7

    with pytest.warns(DeprecationWarning) as record:
        old_fn()

    assert record[0].filename == __file__


def test_alias_access_stacklevel_points_at_the_caller(deprecated_alias) -> None:
    """Same requirement for the module-attribute alias path."""

    class Target:
        pass

    module_getattr = deprecated_alias("OldName", Target, since="3.0.0", removed_in="5.0.0")

    with pytest.warns(DeprecationWarning) as record:
        module_getattr("OldName")

    assert record[0].filename == __file__


# ── behaviour preservation ────────────────────────────────────────────────────


def test_decorated_function_still_returns_its_real_value(deprecated) -> None:
    """A deprecation must never change semantics — only add a warning."""

    @deprecated(since="3.0.0", removed_in="5.0.0", replacement="new_fn")
    def old_fn(a: int, b: int = 2) -> int:
        return a + b

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        assert old_fn(3) == 5
        assert old_fn(3, b=10) == 13


def test_decorated_function_keeps_wraps_metadata(deprecated) -> None:
    """functools.wraps — otherwise the api_surface snapshot and mypy both see a different symbol."""

    @deprecated(since="3.0.0", removed_in="5.0.0", replacement="new_fn")
    def old_fn() -> int:
        """Original docstring."""
        return 7

    assert old_fn.__name__ == "old_fn"
    assert old_fn.__doc__ is not None
    assert "Original docstring." in old_fn.__doc__
    assert hasattr(old_fn, "__wrapped__")


async def test_async_function_survives_the_decorator(deprecated) -> None:
    """The repo is async-heavy — the decorator must not turn a coroutine fn into a plain one."""

    @deprecated(since="3.0.0", removed_in="5.0.0", replacement="new_afn")
    async def old_afn(value: int) -> int:
        return value * 2

    with pytest.warns(DeprecationWarning):
        result = await old_afn(21)

    assert result == 42
    assert inspect.iscoroutinefunction(old_afn)


# ── classes ───────────────────────────────────────────────────────────────────


def test_decorating_a_class_warns_on_instantiation_not_at_decoration(deprecated) -> None:
    """Warning at import time is unactionable noise for anyone who merely imports the module."""

    with warnings.catch_warnings(record=True) as at_decoration:
        warnings.simplefilter("always")

        @deprecated(since="3.0.0", removed_in="5.0.0", replacement="NewCls")
        class OldCls:
            def __init__(self, value: int = 1) -> None:
                self.value = value

    assert [w for w in at_decoration if issubclass(w.category, DeprecationWarning)] == []

    with pytest.warns(DeprecationWarning):
        instance = OldCls(5)

    assert instance.value == 5


def test_decorated_class_is_still_the_same_class_for_isinstance(deprecated) -> None:
    """A subclass-based implementation would break `isinstance` for pre-existing instances."""

    class Base:
        pass

    @deprecated(since="3.0.0", removed_in="5.0.0", replacement="NewCls")
    class OldCls(Base):
        pass

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        instance = OldCls()

    assert isinstance(instance, OldCls)
    assert isinstance(instance, Base)
    assert type(instance) is OldCls


# ── deprecated_alias ──────────────────────────────────────────────────────────


def test_alias_returns_the_identical_target_object(deprecated_alias) -> None:
    """The whole point: `except OldName` must catch something raised as NewName."""

    class NewError(Exception):
        pass

    module_getattr = deprecated_alias("OldError", NewError, since="3.0.0", removed_in="5.0.0")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        resolved = module_getattr("OldError")

    assert resolved is NewError


def test_alias_isinstance_and_except_still_work(deprecated_alias) -> None:
    """Behavioural form of the identity assertion above."""

    class NewError(Exception):
        pass

    module_getattr = deprecated_alias("OldError", NewError, since="3.0.0", removed_in="5.0.0")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        OldError = module_getattr("OldError")

    try:
        raise NewError("boom")
    except OldError as exc:  # must catch — same object
        assert isinstance(exc, NewError)
    else:  # pragma: no cover - defensive
        pytest.fail("OldError did not catch NewError")


def test_alias_message_names_since_removed_in_and_target(deprecated_alias) -> None:
    """Same message contract as the decorator."""

    class NewError(Exception):
        pass

    module_getattr = deprecated_alias("OldError", NewError, since="3.0.0", removed_in="5.0.0")

    with pytest.warns(DeprecationWarning) as record:
        module_getattr("OldError")

    message = str(record[0].message)
    assert "OldError" in message
    assert "NewError" in message
    assert "3.0.0" in message
    assert "5.0.0" in message


def test_alias_raises_attribute_error_for_an_unknown_name(deprecated_alias) -> None:
    """A module __getattr__ must preserve normal AttributeError semantics for other names."""

    class NewError(Exception):
        pass

    module_getattr = deprecated_alias("OldError", NewError, since="3.0.0", removed_in="5.0.0")

    with pytest.raises(AttributeError) as exc:
        module_getattr("TotallyUnrelated")

    assert "TotallyUnrelated" in str(exc.value)


def test_alias_removed_in_is_required(deprecated_alias) -> None:
    """Same authoring-time discipline as the decorator."""

    class NewError(Exception):
        pass

    with pytest.raises(TypeError):
        deprecated_alias("OldError", NewError, since="3.0.0")  # type: ignore[call-arg]


# ── per-call-site warning behaviour ───────────────────────────────────────────


def test_warns_once_per_call_under_simplefilter_always(deprecated) -> None:
    """Do not over-specify dedup: with `always` every call must produce exactly one warning."""

    @deprecated(since="3.0.0", removed_in="5.0.0", replacement="new_fn")
    def old_fn() -> int:
        return 7

    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        old_fn()
        old_fn()
        old_fn()

    deprecations = [w for w in record if issubclass(w.category, DeprecationWarning)]
    assert len(deprecations) == 3


def test_warns_at_least_once_under_the_default_filter(deprecated) -> None:
    """Whatever the dedup mechanism, the first call from a fresh site must always warn."""

    @deprecated(since="3.0.0", removed_in="5.0.0", replacement="new_fn")
    def old_fn() -> int:
        return 7

    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("default")
        old_fn()
        old_fn()

    deprecations = [w for w in record if issubclass(w.category, DeprecationWarning)]
    assert len(deprecations) >= 1


# ── Plan 053 / warn_deprecated() (§D-apikey) ─────────────────────────────────────────────


def _warn_deprecated():
    from varco_core.deprecation import warn_deprecated

    return warn_deprecated


def test_warn_deprecated_is_exported():
    import varco_core.deprecation as module

    assert "warn_deprecated" in module.__all__


def test_warn_deprecated_message_shape():
    warn_deprecated = _warn_deprecated()
    with pytest.warns(
        DeprecationWarning,
        match=r"^Thing\(x=\.\.\.\) is deprecated since 4\.0\.0 and will be removed in 5\.0\.0\. Use Other instead\.$",
    ):
        warn_deprecated("Thing(x=...)", since="4.0.0", removed_in="5.0.0", replacement="Other")


def test_warn_deprecated_blames_caller():
    warn_deprecated = _warn_deprecated()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warn_deprecated("Thing(x=...)", since="4.0.0", removed_in="5.0.0")
    assert len(caught) == 1
    assert caught[0].filename == __file__


def test_warn_deprecated_stacklevel_passthrough():
    # A one-frame helper (like ApiKeyAuth.__init__) passes stacklevel=3 so the
    # warning names ITS caller — this test file.
    warn_deprecated = _warn_deprecated()

    def _one_frame_helper() -> None:
        warn_deprecated("Thing(x=...)", since="4.0.0", removed_in="5.0.0", stacklevel=3)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _one_frame_helper()
    assert len(caught) == 1
    assert caught[0].filename == __file__


def test_warn_deprecated_lands_in_the_ledger():
    # 052's ledger is in the tree: the new helper must record there too.
    from varco_core.deprecation import fired_deprecations, reset_fired_deprecations

    warn_deprecated = _warn_deprecated()
    reset_fired_deprecations()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        warn_deprecated("LedgerThing(x=...)", since="4.0.0", removed_in="5.0.0")
    assert any(r.subject == "LedgerThing(x=...)" for r in fired_deprecations())
    reset_fired_deprecations()
