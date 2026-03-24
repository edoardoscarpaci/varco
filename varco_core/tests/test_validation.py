"""
Unit tests for varco_core.validation
======================================
Covers the business-invariant validation layer.

Sections
--------
- ``ValidationError``       — construction, immutability
- ``ValidationResult``      — ok(), error(), is_valid, __add__, raise_if_invalid,
                              multiple-error join, repr
- ``VALID`` singleton       — is_valid, is exact same object as ok()
- ``Validator`` protocol    — isinstance check
- ``CompositeValidator``    — fan-out, all-pass, collect-all, empty composite
- ``DomainModelValidator``  — abstract enforcement, _rule helper, repr
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from varco_core.exception.service import ServiceValidationError
from varco_core.model import DomainModel
from varco_core.validation import (
    VALID,
    CompositeValidator,
    DomainModelValidator,
    ValidationError,
    ValidationResult,
    Validator,
)


# ── Minimal domain model for tests ───────────────────────────────────────────


@dataclass(kw_only=True)
class Order(DomainModel):
    quantity: int = 1
    total: float = 0.0


# ── ValidationError ───────────────────────────────────────────────────────────


class TestValidationError:
    def test_message_only(self) -> None:
        e = ValidationError(message="qty must be positive")
        assert e.message == "qty must be positive"
        assert e.field is None

    def test_with_field(self) -> None:
        e = ValidationError(message="must be positive", field="quantity")
        assert e.field == "quantity"

    def test_frozen(self) -> None:
        e = ValidationError(message="x")
        with pytest.raises(Exception):
            e.message = "y"  # type: ignore[misc]

    def test_hashable(self) -> None:
        e = ValidationError(message="x", field="f")
        assert hash(e) is not None


# ── ValidationResult ──────────────────────────────────────────────────────────


class TestValidationResult:
    def test_ok_returns_valid_result(self) -> None:
        r = ValidationResult.ok()
        assert r.is_valid is True
        assert r.errors == ()

    def test_ok_returns_valid_singleton(self) -> None:
        # ok() must return the VALID module-level singleton for allocation efficiency
        assert ValidationResult.ok() is VALID

    def test_error_creates_one_error_result(self) -> None:
        r = ValidationResult.error("qty must be positive", field="quantity")
        assert r.is_valid is False
        assert len(r.errors) == 1
        assert r.errors[0].message == "qty must be positive"
        assert r.errors[0].field == "quantity"

    def test_error_without_field(self) -> None:
        r = ValidationResult.error("cross-field violation")
        assert r.errors[0].field is None

    def test_add_two_ok_results(self) -> None:
        r = ValidationResult.ok() + ValidationResult.ok()
        assert r.is_valid is True

    def test_add_ok_plus_error(self) -> None:
        r = ValidationResult.ok() + ValidationResult.error("bad")
        assert r.is_valid is False
        assert len(r.errors) == 1

    def test_add_error_plus_ok(self) -> None:
        r = ValidationResult.error("bad") + ValidationResult.ok()
        assert r.is_valid is False
        assert len(r.errors) == 1

    def test_add_two_errors_combines_all(self) -> None:
        a = ValidationResult.error("rule A", field="field_a")
        b = ValidationResult.error("rule B", field="field_b")
        combined = a + b
        assert len(combined.errors) == 2
        messages = {e.message for e in combined.errors}
        assert "rule A" in messages
        assert "rule B" in messages

    def test_raise_if_invalid_on_ok_is_noop(self) -> None:
        # Must not raise
        ValidationResult.ok().raise_if_invalid()

    def test_raise_if_invalid_single_error_raises(self) -> None:
        r = ValidationResult.error("qty must be positive", field="quantity")
        with pytest.raises(ServiceValidationError) as exc_info:
            r.raise_if_invalid()
        # Single error — field should be passed through
        assert "qty must be positive" in str(exc_info.value)

    def test_raise_if_invalid_multiple_errors_joins_all(self) -> None:
        r = ValidationResult.error("rule A", field="a") + ValidationResult.error(
            "rule B", field="b"
        )
        with pytest.raises(ServiceValidationError) as exc_info:
            r.raise_if_invalid()
        msg = str(exc_info.value)
        assert "rule A" in msg
        assert "rule B" in msg

    def test_frozen(self) -> None:
        r = ValidationResult.error("x")
        with pytest.raises(Exception):
            r.errors = ()  # type: ignore[misc]

    def test_repr_ok(self) -> None:
        assert "ok" in repr(ValidationResult.ok()).lower()

    def test_repr_with_errors(self) -> None:
        r = ValidationResult.error("bad input", field="qty")
        text = repr(r)
        assert "bad input" in text


# ── VALID singleton ───────────────────────────────────────────────────────────


class TestVALIDSingleton:
    def test_is_valid(self) -> None:
        assert VALID.is_valid is True

    def test_is_same_as_ok(self) -> None:
        assert ValidationResult.ok() is VALID


# ── Validator protocol ────────────────────────────────────────────────────────


class TestValidatorProtocol:
    def test_class_with_validate_method_satisfies_protocol(self) -> None:
        class MyValidator:
            def validate(self, value: object) -> ValidationResult:
                return ValidationResult.ok()

        assert isinstance(MyValidator(), Validator)

    def test_class_without_validate_does_not_satisfy(self) -> None:
        class NotAValidator:
            pass

        assert not isinstance(NotAValidator(), Validator)


# ── CompositeValidator ────────────────────────────────────────────────────────


class TestCompositeValidator:
    def _quantity_validator(self) -> Validator[Order]:
        class QuantityValidator:
            def validate(self, value: Order) -> ValidationResult:
                if value.quantity <= 0:
                    return ValidationResult.error(
                        "quantity must be positive", field="quantity"
                    )
                return ValidationResult.ok()

        return QuantityValidator()

    def _total_validator(self) -> Validator[Order]:
        class TotalValidator:
            def validate(self, value: Order) -> ValidationResult:
                if value.total < 0:
                    return ValidationResult.error(
                        "total must be non-negative", field="total"
                    )
                return ValidationResult.ok()

        return TotalValidator()

    def test_empty_composite_always_passes(self) -> None:
        composite: CompositeValidator[Order] = CompositeValidator()
        order = Order(quantity=1, total=10.0)
        result = composite.validate(order)
        assert result.is_valid is True

    def test_all_validators_pass_returns_ok(self) -> None:
        composite = CompositeValidator(
            self._quantity_validator(), self._total_validator()
        )
        result = composite.validate(Order(quantity=1, total=10.0))
        assert result.is_valid is True

    def test_one_validator_fails_returns_errors(self) -> None:
        composite = CompositeValidator(
            self._quantity_validator(), self._total_validator()
        )
        result = composite.validate(Order(quantity=-1, total=10.0))
        assert result.is_valid is False
        assert any("quantity" in e.message for e in result.errors)

    def test_all_validators_run_collect_all_errors(self) -> None:
        """All validators run — fail-fast is NOT the behaviour here."""
        composite = CompositeValidator(
            self._quantity_validator(), self._total_validator()
        )
        # Both qty and total are invalid
        result = composite.validate(Order(quantity=-1, total=-5.0))
        assert len(result.errors) == 2

    def test_repr_contains_child_class_names(self) -> None:
        composite = CompositeValidator(self._quantity_validator())
        assert "CompositeValidator" in repr(composite)


# ── DomainModelValidator ──────────────────────────────────────────────────────


class TestDomainModelValidator:
    def _make_validator(self) -> DomainModelValidator[Order]:
        class OrderValidator(DomainModelValidator[Order]):
            def validate(self, value: Order) -> ValidationResult:
                result = ValidationResult.ok()
                result += self._rule(
                    value.quantity <= 0,
                    f"quantity must be positive (got {value.quantity})",
                    field="quantity",
                )
                result += self._rule(
                    value.total < 0,
                    f"total must be non-negative (got {value.total})",
                    field="total",
                )
                return result

        return OrderValidator()

    def test_passes_valid_entity(self) -> None:
        v = self._make_validator()
        result = v.validate(Order(quantity=5, total=99.9))
        assert result.is_valid is True

    def test_fails_invalid_entity(self) -> None:
        v = self._make_validator()
        result = v.validate(Order(quantity=0, total=-1.0))
        assert result.is_valid is False
        assert len(result.errors) == 2

    def test_rule_helper_returns_ok_when_condition_false(self) -> None:
        class SimpleValidator(DomainModelValidator[Order]):
            def validate(self, value: Order) -> ValidationResult:
                return self._rule(False, "should not appear")

        v = SimpleValidator()
        assert v.validate(Order()).is_valid is True

    def test_rule_helper_returns_error_when_condition_true(self) -> None:
        class SimpleValidator(DomainModelValidator[Order]):
            def validate(self, value: Order) -> ValidationResult:
                return self._rule(True, "violation", field="qty")

        v = SimpleValidator()
        result = v.validate(Order())
        assert result.is_valid is False
        assert result.errors[0].field == "qty"

    def test_abstract_method_enforced(self) -> None:
        """Cannot instantiate DomainModelValidator without implementing validate()."""
        with pytest.raises(TypeError):
            DomainModelValidator()  # type: ignore[abstract]

    def test_repr_contains_class_name(self) -> None:
        v = self._make_validator()
        assert "OrderValidator" in repr(v)

    def test_satisfies_validator_protocol(self) -> None:
        v = self._make_validator()
        assert isinstance(v, Validator)
