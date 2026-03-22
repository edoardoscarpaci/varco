"""
Unit tests for varco_core.registry — DomainModelRegistry and @register.

Uses DomainModelRegistry.clear() in fixtures to prevent cross-test pollution
since the registry is a class-level singleton.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from varco_core.model import DomainModel
from varco_core.registry import DomainModelRegistry, register


# ── Fixture: ensure clean registry for each test ──────────────────────────────


@pytest.fixture(autouse=True)
def clean_registry():
    """Clear the process-level registry before and after every test."""
    DomainModelRegistry.clear()
    yield
    DomainModelRegistry.clear()


# ── DomainModelRegistry ────────────────────────────────────────────────────────


def test_registry_empty_initially():
    assert DomainModelRegistry.all() == []


def test_registry_add_single_class():
    @dataclass
    class _Widget(DomainModel):
        name: str

        class Meta:
            table = "widgets"

    DomainModelRegistry.add(_Widget)
    assert _Widget in DomainModelRegistry.all()


def test_registry_all_preserves_insertion_order():
    @dataclass
    class _A(DomainModel):
        class Meta:
            table = "a"

    @dataclass
    class _B(DomainModel):
        class Meta:
            table = "b"

    DomainModelRegistry.add(_A)
    DomainModelRegistry.add(_B)
    result = DomainModelRegistry.all()
    assert result.index(_A) < result.index(_B)


def test_registry_add_is_idempotent():
    """Adding the same class twice must not create a duplicate entry."""

    @dataclass
    class _C(DomainModel):
        class Meta:
            table = "c"

    DomainModelRegistry.add(_C)
    DomainModelRegistry.add(_C)
    assert DomainModelRegistry.all().count(_C) == 1


def test_registry_clear_empties():
    @dataclass
    class _D(DomainModel):
        class Meta:
            table = "d"

    DomainModelRegistry.add(_D)
    DomainModelRegistry.clear()
    assert DomainModelRegistry.all() == []


def test_registry_returns_copy_not_reference():
    """all() must return a new list — mutations must not affect the registry."""

    @dataclass
    class _E(DomainModel):
        class Meta:
            table = "e"

    DomainModelRegistry.add(_E)
    lst = DomainModelRegistry.all()
    lst.clear()
    # Registry should still contain _E
    assert _E in DomainModelRegistry.all()


# ── @register decorator ────────────────────────────────────────────────────────


def test_register_adds_to_registry():
    @register
    @dataclass
    class _F(DomainModel):
        class Meta:
            table = "f"

    assert _F in DomainModelRegistry.all()


def test_register_returns_class_unchanged():
    """@register must return the class itself — no wrapping."""

    @register
    @dataclass
    class _G(DomainModel):
        class Meta:
            table = "g"

    # Class identity preserved
    assert issubclass(_G, DomainModel)
    assert _G.__name__ == "_G"


def test_register_non_domain_model_raises_type_error():
    """Applying @register to a non-DomainModel class must raise TypeError."""
    with pytest.raises(TypeError, match="DomainModel"):

        @register
        class _NotAModel:
            pass


def test_register_twice_is_idempotent():
    @dataclass
    class _H(DomainModel):
        class Meta:
            table = "h"

    register(_H)
    register(_H)
    assert DomainModelRegistry.all().count(_H) == 1
